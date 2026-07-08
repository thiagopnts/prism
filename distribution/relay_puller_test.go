package distribution

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
	"github.com/zsiec/prism/moq"
	"github.com/zsiec/prism/moqclient"
)

// cancelableReader is a fake uni-stream that records CancelRead, matching the
// method shape dropStream asserts for. It is safe for concurrent use so a reader
// goroutine can CancelRead it while the test observes the result.
type cancelableReader struct {
	mu        sync.Mutex
	cancelled bool
	code      webtransport.StreamErrorCode
}

func (c *cancelableReader) Read([]byte) (int, error) { return 0, io.EOF }
func (c *cancelableReader) CancelRead(e webtransport.StreamErrorCode) {
	c.mu.Lock()
	c.cancelled = true
	c.code = e
	c.mu.Unlock()
}

func (c *cancelableReader) state() (bool, webtransport.StreamErrorCode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cancelled, c.code
}

func TestDropStreamCancelsCancelableStream(t *testing.T) {
	c := &cancelableReader{}
	dropStream(c)
	cancelled, code := c.state()
	if !cancelled {
		t.Fatal("dropStream did not CancelRead a cancelable stream — its flow-control credit would leak")
	}
	if code != 0 {
		t.Errorf("dropStream used error code %d, want 0", code)
	}
	// A reader without CancelRead must be a safe no-op, not a panic.
	dropStream(bytes.NewReader([]byte("plain")))
}

func TestReplayPolicyForTrack(t *testing.T) {
	cases := map[string]RawReplayPolicy{
		"video":    ReplayCurrentGroup,
		"captions": ReplayCurrentGroup,
		"audio0":   ReplayRecentRing,
		"audio11":  ReplayRecentRing,
		"stats":    ReplayNone,
		"control":  ReplayNone,
		"unknown":  ReplayNone,
	}
	for track, want := range cases {
		if got := replayPolicyForTrack(track); got != want {
			t.Errorf("replayPolicyForTrack(%q) = %v, want %v", track, got, want)
		}
	}
}

type ctrlMsg struct {
	typ     uint64
	payload []byte
}

// drainControlMsgs reads every complete control message buffered in buf.
func drainControlMsgs(t *testing.T, buf *bytes.Buffer) []ctrlMsg {
	t.Helper()
	var out []ctrlMsg
	for buf.Len() > 0 {
		mt, payload, err := moq.ReadControlMsg(buf)
		if err != nil {
			t.Fatalf("read control message: %v", err)
		}
		out = append(out, ctrlMsg{typ: mt, payload: payload})
	}
	return out
}

func TestSendSubscribeIdempotentPerConnection(t *testing.T) {
	rw := &mockControlStream{Reader: &bytes.Buffer{}, Writer: &bytes.Buffer{}}
	sess := moqclient.NewSession(rw)
	p := &RelayPuller{
		cfg:     UpstreamConfig{StreamKey: "live"},
		log:     slog.Default(),
		desired: map[string]bool{},
		relay:   NewRelay(),
	}
	p.live = &liveConn{sess: sess, pending: newPendingReqs(), reqByTrack: map[string]uint64{}}

	p.Subscribe("video")
	p.Subscribe("video") // duplicate: must be suppressed

	msgs := drainControlMsgs(t, rw.Writer)
	if len(msgs) != 1 {
		t.Fatalf("control messages written = %d, want 1 (idempotent subscribe)", len(msgs))
	}
	if msgs[0].typ != moq.MsgSubscribe {
		t.Fatalf("message type = %#x, want SUBSCRIBE", msgs[0].typ)
	}
	parsed, err := moq.ParseSubscribe(msgs[0].payload)
	if err != nil {
		t.Fatalf("parse SUBSCRIBE: %v", err)
	}
	if parsed.TrackName != "video" {
		t.Fatalf("track name = %q, want video", parsed.TrackName)
	}
	want := []string{"prism", "live"}
	if len(parsed.Namespace) != 2 || parsed.Namespace[0] != want[0] || parsed.Namespace[1] != want[1] {
		t.Fatalf("namespace = %v, want %v", parsed.Namespace, want)
	}
	if _, ok := p.live.reqByTrack["video"]; !ok {
		t.Fatal("reqByTrack not recorded for video")
	}
}

func TestUnsubscribeReferencesSubscribeRequestID(t *testing.T) {
	rw := &mockControlStream{Reader: &bytes.Buffer{}, Writer: &bytes.Buffer{}}
	sess := moqclient.NewSession(rw)
	p := &RelayPuller{
		cfg:     UpstreamConfig{StreamKey: "live"},
		log:     slog.Default(),
		desired: map[string]bool{},
		relay:   NewRelay(),
	}
	p.live = &liveConn{sess: sess, pending: newPendingReqs(), reqByTrack: map[string]uint64{}}

	p.Subscribe("video")
	subReqID := p.live.reqByTrack["video"]

	p.Unsubscribe("video")

	msgs := drainControlMsgs(t, rw.Writer)
	if len(msgs) != 2 {
		t.Fatalf("control messages = %d, want 2 (subscribe + unsubscribe)", len(msgs))
	}
	if msgs[1].typ != moq.MsgUnsubscribe {
		t.Fatalf("second message type = %#x, want UNSUBSCRIBE", msgs[1].typ)
	}
	unsub, err := moq.ParseUnsubscribe(msgs[1].payload)
	if err != nil {
		t.Fatalf("parse UNSUBSCRIBE: %v", err)
	}
	if unsub.RequestID != subReqID {
		t.Fatalf("unsubscribe requestID = %d, want %d (the subscribe's id)", unsub.RequestID, subReqID)
	}
	if _, ok := p.live.reqByTrack["video"]; ok {
		t.Fatal("reqByTrack still has video after unsubscribe")
	}
	if p.desired["video"] {
		t.Fatal("desired still has video after unsubscribe")
	}
}

func TestReconnectResubscribesDesiredTracks(t *testing.T) {
	rw1 := &mockControlStream{Reader: &bytes.Buffer{}, Writer: &bytes.Buffer{}}
	p := &RelayPuller{
		cfg:     UpstreamConfig{StreamKey: "live"},
		log:     slog.Default(),
		desired: map[string]bool{},
		relay:   NewRelay(),
	}
	p.live = &liveConn{sess: moqclient.NewSession(rw1), pending: newPendingReqs(), reqByTrack: map[string]uint64{}}
	p.Subscribe("video")
	p.Subscribe("audio0")

	// Reconnect: a fresh connection must re-subscribe every desired track. This
	// mirrors the runOnce re-subscribe loop exactly (under p.mu, iterating desired).
	rw2 := &mockControlStream{Reader: &bytes.Buffer{}, Writer: &bytes.Buffer{}}
	p.mu.Lock()
	p.live = &liveConn{sess: moqclient.NewSession(rw2), pending: newPendingReqs(), reqByTrack: map[string]uint64{}}
	for name := range p.desired {
		p.sendSubscribeLocked(name)
	}
	p.mu.Unlock()

	msgs := drainControlMsgs(t, rw2.Writer)
	if len(msgs) != 2 {
		t.Fatalf("re-subscribed %d tracks on reconnect, want 2 (video + audio0)", len(msgs))
	}
	got := map[string]bool{}
	for _, m := range msgs {
		if m.typ != moq.MsgSubscribe {
			t.Fatalf("message type = %#x, want SUBSCRIBE", m.typ)
		}
		parsed, err := moq.ParseSubscribe(m.payload)
		if err != nil {
			t.Fatal(err)
		}
		got[parsed.TrackName] = true
	}
	if !got["video"] || !got["audio0"] {
		t.Fatalf("re-subscribed tracks = %v, want video + audio0", got)
	}
}

func TestReconnectDoesNotResubscribeReleasedTrack(t *testing.T) {
	rw1 := &mockControlStream{Reader: &bytes.Buffer{}, Writer: &bytes.Buffer{}}
	p := &RelayPuller{
		cfg:     UpstreamConfig{StreamKey: "live"},
		log:     slog.Default(),
		desired: map[string]bool{},
		relay:   NewRelay(),
	}
	p.live = &liveConn{sess: moqclient.NewSession(rw1), pending: newPendingReqs(), reqByTrack: map[string]uint64{}}

	p.Subscribe("video")
	// Last viewer leaves and the grace expires → Unsubscribe removes it from desired.
	p.Unsubscribe("video")
	if p.desired["video"] {
		t.Fatal("video should be removed from desired after unsubscribe")
	}

	// Reconnect: the released track must NOT come back on the new connection. This
	// is the Finding-2 invariant: desired and the re-subscribe are one lock domain.
	rw2 := &mockControlStream{Reader: &bytes.Buffer{}, Writer: &bytes.Buffer{}}
	p.mu.Lock()
	p.live = &liveConn{sess: moqclient.NewSession(rw2), pending: newPendingReqs(), reqByTrack: map[string]uint64{}}
	for name := range p.desired {
		p.sendSubscribeLocked(name)
	}
	p.mu.Unlock()

	msgs := drainControlMsgs(t, rw2.Writer)
	if len(msgs) != 0 {
		t.Fatalf("re-subscribed %d tracks on reconnect, want 0 (released track must not return)", len(msgs))
	}
	if _, ok := p.live.reqByTrack["video"]; ok {
		t.Fatal("released track was re-subscribed on the new connection")
	}
}

func TestControlLoopResolvesAlias(t *testing.T) {
	rw := &mockControlStream{Reader: &bytes.Buffer{}, Writer: &bytes.Buffer{}}
	// Pre-load a SUBSCRIBE_OK for request 7 → alias 99.
	sok := moq.SerializeSubscribeOK(moq.SubscribeOK{
		RequestID:  7,
		TrackAlias: 99,
		GroupOrder: moq.GroupOrderAscending,
	})
	if err := moq.WriteControlMsg(rw.Reader, moq.MsgSubscribeOK, sok); err != nil {
		t.Fatal(err)
	}

	sess := moqclient.NewSession(rw)
	pending := newPendingReqs()
	pending.set(7, "video")
	aliases := newTrackAliasMap()
	p := &RelayPuller{log: slog.Default()}

	// controlLoop processes the buffered SUBSCRIBE_OK, then returns on the EOF
	// that follows (the buffer is drained). That EOF is the expected exit.
	_ = p.controlLoop(context.Background(), sess, pending, aliases)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	name, ok := aliases.waitFor(ctx, 99, time.Second)
	if !ok || name != "video" {
		t.Fatalf("alias 99 resolved to (%q, %v), want (video, true)", name, ok)
	}
	if _, ok := pending.take(7); ok {
		t.Fatal("pending request 7 should have been consumed by SUBSCRIBE_OK")
	}
}

func TestControlLoopGoAwayReturnsError(t *testing.T) {
	rw := &mockControlStream{Reader: &bytes.Buffer{}, Writer: &bytes.Buffer{}}
	if err := moq.WriteControlMsg(rw.Reader, moq.MsgGoAway, moq.SerializeGoAway(moq.GoAway{})); err != nil {
		t.Fatal(err)
	}
	sess := moqclient.NewSession(rw)
	p := &RelayPuller{log: slog.Default()}

	err := p.controlLoop(context.Background(), sess, newPendingReqs(), newTrackAliasMap())
	if err == nil {
		t.Fatal("controlLoop returned nil on GOAWAY, want error")
	}
}

// captureViewer records every object fanned out to it.
type captureViewer struct {
	id  string
	mu  sync.Mutex
	got []RawObject
}

func (c *captureViewer) ID() string { return c.id }

func (c *captureViewer) SendObject(o RawObject) {
	c.mu.Lock()
	c.got = append(c.got, o)
	c.mu.Unlock()
}

func (c *captureViewer) objects() []RawObject {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]RawObject, len(c.got))
	copy(out, c.got)
	return out
}

func TestReadStreamForwardsVerbatimWithStartMarker(t *testing.T) {
	// Serialize one upstream subgroup stream: header(alias=1, group=5, sg=0,
	// pri=128) + three objects, the first carrying an extension block.
	var raw bytes.Buffer
	w := NewRawStreamWriter(1)
	if err := w.WriteRawStreamHeader(&raw, 5, 0, 128); err != nil {
		t.Fatal(err)
	}
	ext := rawExtEven(locExtCaptureTimestamp, 123456)
	if _, err := w.WriteRawObject(&raw, 0, ext, []byte("AAA")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteRawObject(&raw, 1, nil, []byte("BB")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteRawObject(&raw, 2, nil, []byte("C")); err != nil {
		t.Fatal(err)
	}

	// The demux loop consumes the subgroup header before handing off; mirror that.
	r := bufio.NewReader(&raw)
	hdr, err := moqclient.ReadSubgroupHeader(r)
	if err != nil {
		t.Fatal(err)
	}

	relay := NewRelay()
	relay.EnsureRawTrack("video", ReplayCurrentGroup)
	cap := &captureViewer{id: "v1"}
	relay.AddRawViewer("video", cap)

	p := &RelayPuller{relay: relay, log: slog.Default()}
	p.readStream(context.Background(), "video", headeredStream{hdr: hdr, r: r})

	got := cap.objects()
	if len(got) != 3 {
		t.Fatalf("forwarded objects = %d, want 3", len(got))
	}
	if !got[0].StartsStream {
		t.Error("first object should be marked StartsStream")
	}
	if got[1].StartsStream || got[2].StartsStream {
		t.Error("only the first object should be marked StartsStream")
	}
	for i, want := range []struct {
		id      uint64
		payload string
	}{{0, "AAA"}, {1, "BB"}, {2, "C"}} {
		if got[i].ObjectID != want.id {
			t.Errorf("object %d ID = %d, want %d", i, got[i].ObjectID, want.id)
		}
		if string(got[i].Payload) != want.payload {
			t.Errorf("object %d payload = %q, want %q", i, got[i].Payload, want.payload)
		}
		if got[i].GroupID != 5 || got[i].SubgroupID != 0 || got[i].Priority != 128 {
			t.Errorf("object %d header = (g=%d sg=%d pri=%d), want (5,0,128)",
				i, got[i].GroupID, got[i].SubgroupID, got[i].Priority)
		}
	}
	if !bytes.Equal(got[0].ExtBytes, ext) {
		t.Errorf("first object ext = %x, want %x (verbatim)", got[0].ExtBytes, ext)
	}
	if got[1].ExtBytes != nil {
		t.Errorf("second object ext = %x, want nil", got[1].ExtBytes)
	}
}

func TestReadStreamCatalogGoesToSetCatalog(t *testing.T) {
	var raw bytes.Buffer
	w := NewRawStreamWriter(2)
	if err := w.WriteRawStreamHeader(&raw, 0, 0, 128); err != nil {
		t.Fatal(err)
	}
	catalog := []byte(`{"tracks":["video"]}`)
	if _, err := w.WriteRawObject(&raw, 0, nil, catalog); err != nil {
		t.Fatal(err)
	}

	r := bufio.NewReader(&raw)
	if _, err := moqclient.ReadSubgroupHeader(r); err != nil {
		t.Fatal(err)
	}

	relay := NewRelay()
	p := &RelayPuller{relay: relay, log: slog.Default()}
	p.readStream(context.Background(), catalogTrack, headeredStream{r: r})

	if got := relay.Catalog(); !bytes.Equal(got, catalog) {
		t.Fatalf("relay catalog = %q, want %q", got, catalog)
	}
	// The catalog must not be registered as a raw media track.
	if relay.RawViewerCount(catalogTrack) != 0 {
		t.Fatal("catalog should not create a raw track")
	}
}

// oneObjectStream serializes a single-object subgroup stream for the given alias
// and group, consumes its header (as the demux loop does), and returns the
// headeredStream ready to hand to a track reader.
func oneObjectStream(t *testing.T, alias, group uint64, payload string) headeredStream {
	t.Helper()
	var raw bytes.Buffer
	w := NewRawStreamWriter(alias)
	if err := w.WriteRawStreamHeader(&raw, group, 0, 128); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteRawObject(&raw, 0, nil, []byte(payload)); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(&raw)
	hdr, err := moqclient.ReadSubgroupHeader(r)
	if err != nil {
		t.Fatal(err)
	}
	return headeredStream{hdr: hdr, r: r, stream: &cancelableReader{}}
}

// TestTrackReaderResolvesAliasAfterDispatch is the core of the non-blocking
// alias change: a stream may be dispatched before its SUBSCRIBE_OK resolves the
// alias, and the reader — not the shared demux loop — waits for it. The alias is
// set only after both streams are dispatched, proving dispatch did not block on
// resolution, and both streams are then read in accept order.
func TestTrackReaderResolvesAliasAfterDispatch(t *testing.T) {
	relay := NewRelay()
	relay.EnsureRawTrack("video", ReplayCurrentGroup)
	cap := &captureViewer{id: "v1"}
	relay.AddRawViewer("video", cap)

	p := &RelayPuller{relay: relay, log: slog.Default()}
	aliases := newTrackAliasMap()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readers := newTrackReaderSet(ctx, p, aliases)
	defer readers.shutdown()

	// Dispatch two streams for alias 1 BEFORE the alias resolves. If dispatch
	// blocked on resolution this would deadlock the test.
	readers.dispatch(1, oneObjectStream(t, 1, 5, "AAA"))
	readers.dispatch(1, oneObjectStream(t, 1, 6, "BBB"))

	aliases.set(1, "video")

	eventually(t, 2*time.Second, func() bool { return len(cap.objects()) == 2 })

	got := cap.objects()
	if got[0].GroupID != 5 || got[1].GroupID != 6 {
		t.Fatalf("objects forwarded out of accept order: groups = %d, %d, want 5, 6", got[0].GroupID, got[1].GroupID)
	}
}

// TestTrackReaderDropsStreamsForUnresolvedAlias verifies that a stream whose
// alias never resolves is dropped with CancelRead (returning its flow-control
// credit) rather than leaked, and without blocking the demux loop.
func TestTrackReaderDropsStreamsForUnresolvedAlias(t *testing.T) {
	p := &RelayPuller{relay: NewRelay(), log: slog.Default()}
	aliases := newTrackAliasMap()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readers := newTrackReaderSet(ctx, p, aliases)
	readers.waitTimeout = 20 * time.Millisecond
	defer readers.shutdown()

	cr := &cancelableReader{}
	readers.dispatch(7, headeredStream{stream: cr})

	eventually(t, 2*time.Second, func() bool {
		cancelled, _ := cr.state()
		return cancelled
	})
}
