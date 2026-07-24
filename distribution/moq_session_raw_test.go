package distribution

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zsiec/prism/moq"
	"github.com/zsiec/prism/moqclient"
)

// recStream is an in-memory io.WriteCloser capturing one downstream uni-stream.
type recStream struct {
	buf    bytes.Buffer
	closed bool
}

func (s *recStream) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *recStream) Close() error                { s.closed = true; return nil }

// decodeRawStream parses a captured downstream uni-stream into its header and
// objects, verifying the verbatim framing round-trips through the real reader.
func decodeRawStream(t *testing.T, data []byte) (moqclient.SubgroupHeader, []moqclient.Object) {
	t.Helper()
	r := bufio.NewReader(bytes.NewReader(data))
	hdr, err := moqclient.ReadSubgroupHeader(r)
	if err != nil {
		t.Fatalf("decode subgroup header: %v", err)
	}
	var objs []moqclient.Object
	for {
		o, err := moqclient.ReadObject(r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("decode object: %v", err)
			}
			break
		}
		objs = append(objs, o)
	}
	return hdr, objs
}

func TestRawStreamMuxOpensStreamPerUpstreamStream(t *testing.T) {
	var opened []*recStream
	mx := &rawStreamMux{
		w: NewRawStreamWriter(42),
		open: func() (io.WriteCloser, error) {
			s := &recStream{}
			opened = append(opened, s)
			return s, nil
		},
	}

	objs := []RawObject{
		{StartsStream: true, GroupID: 5, SubgroupID: 0, Priority: 128, ObjectID: 0, Payload: []byte("A")},
		{GroupID: 5, SubgroupID: 0, Priority: 128, ObjectID: 1, Payload: []byte("B")},
		{StartsStream: true, GroupID: 6, SubgroupID: 0, Priority: 128, ObjectID: 0, Payload: []byte("C")},
	}
	for _, o := range objs {
		if _, err := mx.write(o); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	mx.closeCurrent()

	if len(opened) != 2 {
		t.Fatalf("opened %d downstream streams, want 2 (one per StartsStream)", len(opened))
	}
	for i, s := range opened {
		if !s.closed {
			t.Errorf("downstream stream %d was not closed", i)
		}
	}

	h0, o0 := decodeRawStream(t, opened[0].buf.Bytes())
	if h0.TrackAlias != 42 || h0.GroupID != 5 || h0.SubgroupID != 0 || h0.Priority != 128 {
		t.Fatalf("stream 0 header = %+v, want alias 42 group 5", h0)
	}
	if len(o0) != 2 || string(o0[0].Payload) != "A" || string(o0[1].Payload) != "B" {
		t.Fatalf("stream 0 objects = %+v, want A,B", o0)
	}
	if o0[0].ObjectID != 0 || o0[1].ObjectID != 1 {
		t.Fatalf("stream 0 object IDs = %d,%d, want 0,1", o0[0].ObjectID, o0[1].ObjectID)
	}

	h1, o1 := decodeRawStream(t, opened[1].buf.Bytes())
	if h1.GroupID != 6 {
		t.Fatalf("stream 1 group = %d, want 6", h1.GroupID)
	}
	if len(o1) != 1 || string(o1[0].Payload) != "C" {
		t.Fatalf("stream 1 objects = %+v, want C", o1)
	}
}

func TestRawStreamMuxFirstObjectWithoutMarkerOpensStream(t *testing.T) {
	// An audio late-joiner replays mid-stream objects, none marked StartsStream;
	// the mux must still open a stream for the first one.
	var opened int
	mx := &rawStreamMux{
		w:    NewRawStreamWriter(7),
		open: func() (io.WriteCloser, error) { opened++; return &recStream{}, nil },
	}
	for i := 0; i < 3; i++ {
		if _, err := mx.write(RawObject{GroupID: 0, ObjectID: uint64(i), Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if opened != 1 {
		t.Fatalf("opened %d streams, want 1 (boundary-less audio stays on one stream)", opened)
	}
}

func TestRawStreamMuxOpenErrorPropagates(t *testing.T) {
	wantErr := errors.New("open failed")
	mx := &rawStreamMux{
		w:    NewRawStreamWriter(1),
		open: func() (io.WriteCloser, error) { return nil, wantErr },
	}
	if _, err := mx.write(RawObject{StartsStream: true}); !errors.Is(err, wantErr) {
		t.Fatalf("write error = %v, want %v", err, wantErr)
	}
}

func TestIsRelayableTrack(t *testing.T) {
	relayable := []string{"video", "captions", "stats", "audio0", "audio3", "audio12", "audio0-eng", "audio1-es"}
	for _, n := range relayable {
		if !isRelayableTrack(n) {
			t.Errorf("isRelayableTrack(%q) = false, want true", n)
		}
	}
	notRelayable := []string{"catalog", "control", "audio", "audiox", "audio0eng", "bogus", ""}
	for _, n := range notRelayable {
		if isRelayableTrack(n) {
			t.Errorf("isRelayableTrack(%q) = true, want false", n)
		}
	}
}

// newRawSession builds a MoQ session bound to a relayed relay, plus the response
// buffer its control writes land in.
func newRawSession(t *testing.T, relay *Relay) (*MoQSession, *bytes.Buffer) {
	t.Helper()
	resp := &bytes.Buffer{}
	ctrl := &mockControlStream{Reader: &bytes.Buffer{}, Writer: resp}
	sess := &MoQSession{
		id:            "raw-session",
		streamKey:     "live",
		control:       ctrl,
		log:           slog.With("session", "raw-session"),
		relay:         relay,
		subscriptions: make(map[string]*moqTrackSub),
	}
	return sess, resp
}

func TestRelayedStreamForksToRawPath(t *testing.T) {
	p := newFakePuller()
	relay := newRelayedRelay(p, time.Hour)
	p.relay = relay

	sess, resp := newRawSession(t, relay)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess.handleSubscribe(ctx, moq.Subscribe{
		RequestID:  1,
		Namespace:  []string{"prism", "live"},
		TrackName:  "video",
		FilterType: moq.FilterLatestObject,
	})

	mt, _, err := moq.ReadControlMsg(resp)
	if err != nil {
		t.Fatal(err)
	}
	if mt != moq.MsgSubscribeOK {
		t.Fatalf("response type = %#x, want SUBSCRIBE_OK", mt)
	}

	sess.mu.RLock()
	sub := sess.subscriptions["video"]
	sess.mu.RUnlock()
	if sub == nil {
		t.Fatal("raw subscription not created")
	}
	if sub.rawCh == nil {
		t.Fatal("raw subscription has no rawCh (took the media path?)")
	}
	if sub.videoCh != nil {
		t.Fatal("raw subscription should not allocate a media video channel")
	}
	if p.subCount("video") != 1 {
		t.Fatalf("upstream subscribe count = %d, want 1", p.subCount("video"))
	}
	if relay.RawViewerCount("video") != 1 {
		t.Fatalf("raw viewer count = %d, want 1", relay.RawViewerCount("video"))
	}
}

func TestRelayedStreamRejectsControlTrack(t *testing.T) {
	p := newFakePuller()
	relay := newRelayedRelay(p, time.Hour)
	p.relay = relay

	sess, resp := newRawSession(t, relay)
	sess.handleSubscribe(context.Background(), moq.Subscribe{
		RequestID:  2,
		Namespace:  []string{"prism", "live"},
		TrackName:  "control",
		FilterType: moq.FilterLatestObject,
	})

	mt, payload, err := moq.ReadControlMsg(resp)
	if err != nil {
		t.Fatal(err)
	}
	if mt != moq.MsgSubscribeError {
		t.Fatalf("response type = %#x, want SUBSCRIBE_ERROR for control on a relayed stream", mt)
	}
	_, off := readVarint(payload, 0) // requestID
	errCode, _ := readVarint(payload, off)
	if errCode != 404 {
		t.Fatalf("error code = %d, want 404", errCode)
	}
	if p.subCount("control") != 0 {
		t.Fatal("control track should not trigger an upstream subscribe")
	}
}

func TestRawUnsubscribeReleasesTrack(t *testing.T) {
	p := newFakePuller()
	relay := newRelayedRelay(p, 15*time.Millisecond)
	p.relay = relay

	sess, resp := newRawSession(t, relay)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess.handleSubscribe(ctx, moq.Subscribe{
		RequestID:  1,
		Namespace:  []string{"prism", "live"},
		TrackName:  "video",
		FilterType: moq.FilterLatestObject,
	})
	if _, _, err := moq.ReadControlMsg(resp); err != nil {
		t.Fatal(err)
	}

	sess.handleUnsubscribe(moq.Unsubscribe{RequestID: 1})

	sess.mu.RLock()
	_, exists := sess.subscriptions["video"]
	sess.mu.RUnlock()
	if exists {
		t.Fatal("subscription should be removed after unsubscribe")
	}
	if relay.RawViewerCount("video") != 0 {
		t.Fatalf("raw viewer count = %d after unsubscribe, want 0", relay.RawViewerCount("video"))
	}
	eventually(t, time.Second, func() bool { return p.unsubCount("video") == 1 })
}

func TestRawSessionRunTeardownReleasesTracks(t *testing.T) {
	p := newFakePuller()
	relay := newRelayedRelay(p, 15*time.Millisecond)
	p.relay = relay

	sess, resp := newRawSession(t, relay)
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	sess.handleSubscribe(subCtx, moq.Subscribe{
		RequestID:  1,
		Namespace:  []string{"prism", "live"},
		TrackName:  "video",
		FilterType: moq.FilterLatestObject,
	})
	if _, _, err := moq.ReadControlMsg(resp); err != nil {
		t.Fatal(err)
	}
	if relay.RawViewerCount("video") != 1 {
		t.Fatalf("raw viewer count = %d before teardown, want 1", relay.RawViewerCount("video"))
	}

	// Run with an already-cancelled context drives straight to teardown.
	runCtx, runCancel := context.WithCancel(context.Background())
	runCancel()
	_ = sess.Run(runCtx)

	if relay.RawViewerCount("video") != 0 {
		t.Fatalf("raw viewer count = %d after teardown, want 0 (leak)", relay.RawViewerCount("video"))
	}
	eventually(t, time.Second, func() bool { return p.unsubCount("video") == 1 })
}

// drainObjectIDs empties ch and returns the ObjectIDs it held, in order.
func drainObjectIDs(ch chan RawObject) []uint64 {
	var ids []uint64
	for {
		select {
		case o := <-ch:
			ids = append(ids, o.ObjectID)
		default:
			return ids
		}
	}
}

// A GOP-aligned viewer that overflows mid-group must shed the rest of that group,
// not just the object that overflowed: enqueuing a later delta whose reference
// frame was dropped would orphan it downstream. Shedding resumes at the keyframe
// (StartsStream) that opens the next group's stream.
func TestRawObjectViewerGOPAlignedShedsWholeGroup(t *testing.T) {
	ch := make(chan RawObject, 2)
	var dropped atomic.Int64
	v := &rawObjectViewer{id: "v", ch: ch, dropped: &dropped, gopAligned: true}

	// Group 1: keyframe + 3 deltas. Two fit; the object that overflows and every
	// later delta of the group are dropped.
	v.SendObject(RawObject{GroupID: 1, ObjectID: 0, StartsStream: true})
	v.SendObject(RawObject{GroupID: 1, ObjectID: 1})
	v.SendObject(RawObject{GroupID: 1, ObjectID: 2})
	v.SendObject(RawObject{GroupID: 1, ObjectID: 3})

	if got := dropped.Load(); got != 2 {
		t.Fatalf("dropped = %d, want 2 (overflow + rest of group)", got)
	}
	if got := drainObjectIDs(ch); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("buffered = %v, want a clean prefix [0 1]", got)
	}

	// Group 2 keyframe: buffer now drained, so shedding must resume and deliver it.
	v.SendObject(RawObject{GroupID: 2, ObjectID: 0, StartsStream: true})
	v.SendObject(RawObject{GroupID: 2, ObjectID: 1})
	if got := dropped.Load(); got != 2 {
		t.Fatalf("dropped after resume = %d, want 2 (no new drops)", got)
	}
	if got := drainObjectIDs(ch); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("group 2 buffered = %v, want [0 1]", got)
	}
}

// While shedding a group, a mid-group delta must not resume delivery even if the
// buffer has since drained — only a keyframe (StartsStream) does. Otherwise a
// delta would be delivered without the group's earlier frames.
func TestRawObjectViewerGOPAlignedResumesOnlyAtKeyframe(t *testing.T) {
	ch := make(chan RawObject, 2)
	var dropped atomic.Int64
	v := &rawObjectViewer{id: "v", ch: ch, dropped: &dropped, gopAligned: true}

	v.SendObject(RawObject{GroupID: 1, ObjectID: 0, StartsStream: true})
	v.SendObject(RawObject{GroupID: 1, ObjectID: 1})
	v.SendObject(RawObject{GroupID: 1, ObjectID: 2}) // overflow -> start shedding
	drainObjectIDs(ch)                               // buffer now has room

	v.SendObject(RawObject{GroupID: 1, ObjectID: 3}) // mid-group: still dropped
	if got := dropped.Load(); got != 2 {
		t.Fatalf("dropped = %d, want 2 (mid-group delta not resumed)", got)
	}
	if got := drainObjectIDs(ch); len(got) != 0 {
		t.Fatalf("buffered = %v, want empty (no mid-group resume)", got)
	}

	v.SendObject(RawObject{GroupID: 2, ObjectID: 0, StartsStream: true})
	if got := drainObjectIDs(ch); len(got) != 1 || got[0] != 0 {
		t.Fatalf("buffered = %v, want [0] (resumed at keyframe)", got)
	}
}

// A non-GOP-aligned viewer (audio's single boundary-less stream, stats) keeps the
// plain drop-one behavior: an overflow drops just that object and delivery
// resumes immediately, since each object is independently decodable.
func TestRawObjectViewerNonAlignedDropsSingleObject(t *testing.T) {
	ch := make(chan RawObject, 1)
	var dropped atomic.Int64
	v := &rawObjectViewer{id: "v", ch: ch, dropped: &dropped, gopAligned: false}

	v.SendObject(RawObject{ObjectID: 0}) // fills buffer
	v.SendObject(RawObject{ObjectID: 1}) // overflow -> dropped
	if got := dropped.Load(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}

	<-ch                                 // make room
	v.SendObject(RawObject{ObjectID: 2}) // delivered immediately (no sticky shedding)
	if got := dropped.Load(); got != 1 {
		t.Fatalf("dropped = %d, want 1 (no extra drop)", got)
	}
	if got := drainObjectIDs(ch); len(got) != 1 || got[0] != 2 {
		t.Fatalf("buffered = %v, want [2]", got)
	}
}
