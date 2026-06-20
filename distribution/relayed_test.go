package distribution

import (
	"bufio"
	"bytes"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/quic-go/quic-go/quicvarint"
	"github.com/zsiec/prism/moqclient"
)

// mockRawViewer implements RawViewer for testing, recording every object sent.
type mockRawViewer struct {
	id  string
	mu  sync.Mutex
	got []RawObject
}

func newMockRawViewer(id string) *mockRawViewer { return &mockRawViewer{id: id} }

func (m *mockRawViewer) ID() string { return m.id }

func (m *mockRawViewer) SendObject(o RawObject) {
	m.mu.Lock()
	m.got = append(m.got, o)
	m.mu.Unlock()
}

func (m *mockRawViewer) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.got)
}

// objectIDs returns the ObjectID of every object received, in order.
func (m *mockRawViewer) objectIDs() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]uint64, len(m.got))
	for i, o := range m.got {
		ids[i] = o.ObjectID
	}
	return ids
}

// rawObj is a small constructor for test objects.
func rawObj(groupID, objectID uint64) RawObject {
	return RawObject{GroupID: groupID, SubgroupID: 0, ObjectID: objectID, Priority: 128}
}

// rawExtEven encodes one even-parity LOC extension (varint value).
func rawExtEven(id, value uint64) []byte {
	b := quicvarint.Append(nil, id)
	return quicvarint.Append(b, value)
}

// rawExtOdd encodes one odd-parity LOC extension (length-prefixed bytes).
func rawExtOdd(id uint64, data []byte) []byte {
	b := quicvarint.Append(nil, id)
	b = quicvarint.Append(b, uint64(len(data)))
	return append(b, data...)
}

// TestRawStreamWriterRoundTrip writes a subgroup stream with RawStreamWriter and
// reads it back with the moqclient reader, proving the framing is byte-exact:
// object IDs, the extension block (including odd / unknown extensions), and the
// payload all survive verbatim, while the session-scoped track alias is the
// downstream value the writer was given.
func TestRawStreamWriterRoundTrip(t *testing.T) {
	t.Parallel()

	const (
		downstreamAlias = uint64(99)
		groupID         = uint64(7)
		subgroupID      = uint64(3)
		priority        = byte(128)
	)

	// A keyframe object with mixed extensions, including an unknown odd one the
	// relay must preserve without understanding it; a delta object; and an object
	// with no extensions at all.
	keyframeExts := rawExtEven(2, 1234567) // capture timestamp
	keyframeExts = append(keyframeExts, rawExtEven(4, 0xE0)...)
	keyframeExts = append(keyframeExts, rawExtOdd(13, []byte{0xAA, 0xBB, 0xCC})...) // video config
	keyframeExts = append(keyframeExts, rawExtOdd(101, []byte{0x01, 0x02})...)      // unknown odd

	objs := []RawObject{
		{GroupID: groupID, SubgroupID: subgroupID, ObjectID: 41, Priority: priority,
			ExtBytes: keyframeExts, Payload: []byte{0x00, 0x00, 0x00, 0x05, 0x65, 0x01, 0x02, 0x03, 0x04}},
		{GroupID: groupID, SubgroupID: subgroupID, ObjectID: 42, Priority: priority,
			ExtBytes: rawExtEven(2, 1234600), Payload: []byte{0x11, 0x22}},
		{GroupID: groupID, SubgroupID: subgroupID, ObjectID: 43, Priority: priority,
			ExtBytes: nil, Payload: []byte{0xFF}},
	}

	var buf bytes.Buffer
	w := NewRawStreamWriter(downstreamAlias)
	if err := w.WriteRawStreamHeader(&buf, groupID, subgroupID, priority); err != nil {
		t.Fatalf("WriteRawStreamHeader: %v", err)
	}
	for _, o := range objs {
		if _, err := w.WriteRawObject(&buf, o.ObjectID, o.ExtBytes, o.Payload); err != nil {
			t.Fatalf("WriteRawObject(%d): %v", o.ObjectID, err)
		}
	}

	r := bufio.NewReader(bytes.NewReader(buf.Bytes()))

	hdr, err := moqclient.ReadSubgroupHeader(r)
	if err != nil {
		t.Fatalf("ReadSubgroupHeader: %v", err)
	}
	if hdr.TrackAlias != downstreamAlias {
		t.Errorf("TrackAlias = %d, want %d (downstream session alias)", hdr.TrackAlias, downstreamAlias)
	}
	if hdr.GroupID != groupID || hdr.SubgroupID != subgroupID || hdr.Priority != priority {
		t.Errorf("header = %+v, want group=%d subgroup=%d priority=%d", hdr, groupID, subgroupID, priority)
	}

	for i, want := range objs {
		got, err := moqclient.ReadObject(r)
		if err != nil {
			t.Fatalf("ReadObject #%d: %v", i, err)
		}
		if got.ObjectID != want.ObjectID {
			t.Errorf("obj #%d ObjectID = %d, want %d", i, got.ObjectID, want.ObjectID)
		}
		if !bytes.Equal(got.Payload, want.Payload) {
			t.Errorf("obj #%d Payload = %v, want %v", i, got.Payload, want.Payload)
		}
		// The extension block must come back byte-for-byte. An object with no
		// extensions must round-trip to a nil RawExtBytes (not an empty slice),
		// matching what moqclient.ReadObject produces for extByteLen==0.
		if len(want.ExtBytes) == 0 {
			if got.RawExtBytes != nil {
				t.Errorf("obj #%d RawExtBytes = %v, want nil for an object with no extensions", i, got.RawExtBytes)
			}
		} else if !bytes.Equal(got.RawExtBytes, want.ExtBytes) {
			t.Errorf("obj #%d RawExtBytes = %v, want %v (verbatim)", i, got.RawExtBytes, want.ExtBytes)
		}
	}

	if _, err := moqclient.ReadObject(r); err != io.EOF {
		t.Fatalf("trailing ReadObject err = %v, want io.EOF", err)
	}
}

// TestBroadcastObjectFanOut verifies every subscribed raw viewer receives each
// broadcast object, and that RemoveRawViewer stops delivery.
func TestBroadcastObjectFanOut(t *testing.T) {
	t.Parallel()

	r := NewRelay()
	r.EnsureRawTrack("video", ReplayCurrentGroup)

	v1, v2 := newMockRawViewer("v1"), newMockRawViewer("v2")
	r.AddRawViewer("video", v1)
	r.AddRawViewer("video", v2)
	if n := r.RawViewerCount("video"); n != 2 {
		t.Fatalf("RawViewerCount = %d, want 2", n)
	}

	r.BroadcastObject("video", rawObj(1, 0))
	r.BroadcastObject("video", rawObj(1, 1))
	if v1.count() != 2 || v2.count() != 2 {
		t.Fatalf("after 2 broadcasts: v1=%d v2=%d, want 2 each", v1.count(), v2.count())
	}

	r.RemoveRawViewer("video", "v1")
	if n := r.RawViewerCount("video"); n != 1 {
		t.Fatalf("RawViewerCount after remove = %d, want 1", n)
	}
	r.BroadcastObject("video", rawObj(1, 2))
	if v1.count() != 2 {
		t.Errorf("removed viewer v1 still received objects: count=%d, want 2", v1.count())
	}
	// v2 must have every object, in broadcast order — not merely the right count.
	if got := v2.objectIDs(); !reflect.DeepEqual(got, []uint64{0, 1, 2}) {
		t.Errorf("v2 received %v, want [0 1 2] in order", got)
	}
}

// TestAddRawViewerReplaysCurrentGroup verifies a late joiner replays exactly the
// current group, and that a GroupID change resets the buffer so a still-later
// joiner only replays the new group.
func TestAddRawViewerReplaysCurrentGroup(t *testing.T) {
	t.Parallel()

	r := NewRelay()
	r.EnsureRawTrack("video", ReplayCurrentGroup)

	// Group 1: three objects, buffered before any viewer joins.
	r.BroadcastObject("video", rawObj(1, 0))
	r.BroadcastObject("video", rawObj(1, 1))
	r.BroadcastObject("video", rawObj(1, 2))

	v1 := newMockRawViewer("v1")
	r.AddRawViewer("video", v1)
	if got := v1.objectIDs(); len(got) != 3 || got[0] != 0 || got[2] != 2 {
		t.Fatalf("v1 replay = %v, want the 3 objects of group 1", got)
	}

	// A live object in the same group reaches the registered viewer.
	r.BroadcastObject("video", rawObj(1, 3))
	if v1.count() != 4 {
		t.Fatalf("v1 count after live object = %d, want 4", v1.count())
	}

	// A new group resets the replay buffer.
	r.BroadcastObject("video", rawObj(2, 0))
	v2 := newMockRawViewer("v2")
	r.AddRawViewer("video", v2)
	if got := v2.objectIDs(); len(got) != 1 || got[0] != 0 {
		t.Fatalf("v2 replay = %v, want only the first object of group 2", got)
	}
	// v1, already registered, saw the group-2 object live (5 total).
	if v1.count() != 5 {
		t.Fatalf("v1 count after group change = %d, want 5", v1.count())
	}
}

// TestReplayRecentRingBounded verifies the audio-shaped policy keeps only the
// most recent rawReplayRingSize objects (no group boundaries on a persistent
// audio subgroup).
func TestReplayRecentRingBounded(t *testing.T) {
	t.Parallel()

	r := NewRelay()
	r.EnsureRawTrack("audio0", ReplayRecentRing)

	const total = rawReplayRingSize + 10
	for i := 0; i < total; i++ {
		r.BroadcastObject("audio0", rawObj(0, uint64(i))) // all groupID 0, as prism writes audio
	}

	v := newMockRawViewer("v1")
	r.AddRawViewer("audio0", v)

	got := v.objectIDs()
	if len(got) != rawReplayRingSize {
		t.Fatalf("replay length = %d, want %d (bounded ring)", len(got), rawReplayRingSize)
	}
	// The ring holds the LAST rawReplayRingSize objects, in order.
	if got[0] != uint64(total-rawReplayRingSize) {
		t.Errorf("first replayed ObjectID = %d, want %d", got[0], total-rawReplayRingSize)
	}
	if got[len(got)-1] != uint64(total-1) {
		t.Errorf("last replayed ObjectID = %d, want %d", got[len(got)-1], total-1)
	}
}

// TestReplayNoneKeepsNoBuffer verifies a no-replay track delivers only live
// objects to a joiner, never a backlog.
func TestReplayNoneKeepsNoBuffer(t *testing.T) {
	t.Parallel()

	r := NewRelay()
	r.EnsureRawTrack("stats", ReplayNone)

	for i := 0; i < 5; i++ {
		r.BroadcastObject("stats", rawObj(0, uint64(i)))
	}

	v := newMockRawViewer("v1")
	r.AddRawViewer("stats", v)
	if v.count() != 0 {
		t.Fatalf("ReplayNone joiner got %d objects on join, want 0", v.count())
	}

	r.BroadcastObject("stats", rawObj(0, 5))
	if got := v.objectIDs(); len(got) != 1 || got[0] != 5 {
		t.Fatalf("after one live object, joiner got %v, want [5]", got)
	}
}

// TestRawTracksAreIndependent verifies fan-out and replay are isolated per
// track key on the same relay.
func TestRawTracksAreIndependent(t *testing.T) {
	t.Parallel()

	r := NewRelay()
	r.EnsureRawTrack("video", ReplayCurrentGroup)
	r.EnsureRawTrack("audio0", ReplayRecentRing)

	vv := newMockRawViewer("vv")
	va := newMockRawViewer("va")
	r.AddRawViewer("video", vv)
	r.AddRawViewer("audio0", va)

	// Distinct object IDs per track so a cross-track delivery bug is visible,
	// not masked by coincidentally-equal counts.
	r.BroadcastObject("video", rawObj(1, 100))
	r.BroadcastObject("audio0", rawObj(0, 0))
	r.BroadcastObject("audio0", rawObj(0, 1))

	if got := vv.objectIDs(); !reflect.DeepEqual(got, []uint64{100}) {
		t.Errorf("video viewer received %v, want [100] only (no audio objects)", got)
	}
	if got := va.objectIDs(); !reflect.DeepEqual(got, []uint64{0, 1}) {
		t.Errorf("audio viewer received %v, want [0 1] only (no video object)", got)
	}
}

// TestReplayRecentRingExactCapacity verifies the ring at exactly its capacity
// holds every object (no premature eviction at the capacity boundary).
func TestReplayRecentRingExactCapacity(t *testing.T) {
	t.Parallel()

	r := NewRelay()
	r.EnsureRawTrack("audio0", ReplayRecentRing)

	for i := 0; i < rawReplayRingSize; i++ {
		r.BroadcastObject("audio0", rawObj(0, uint64(i)))
	}

	v := newMockRawViewer("v1")
	r.AddRawViewer("audio0", v)

	got := v.objectIDs()
	if len(got) != rawReplayRingSize {
		t.Fatalf("replay length = %d, want %d (exactly at capacity)", len(got), rawReplayRingSize)
	}
	if got[0] != 0 || got[len(got)-1] != uint64(rawReplayRingSize-1) {
		t.Errorf("ring at capacity = [%d..%d], want [0..%d]", got[0], got[len(got)-1], rawReplayRingSize-1)
	}
}

// TestConcurrentJoinDuringBroadcast stresses the replay-before-register
// atomicity: a viewer joining concurrently with a stream of broadcasts must end
// up with a complete, gap-free, in-order run — never a missed or duplicated
// object across the join. Because the whole run shares one GroupID,
// ReplayCurrentGroup buffers every object, so the joiner must observe exactly
// [0..n-1] no matter when AddRawViewer interleaves with the broadcasts. If the
// single-lock atomicity were broken (replay and register split across two
// locks), an object broadcast in the gap would be dropped and this would fail
// under repeated -race runs. Concurrent RawViewerCount polling exercises the
// RLock path against the writers.
func TestConcurrentJoinDuringBroadcast(t *testing.T) {
	t.Parallel()

	const n = 500
	if n > rawReplayMaxGroupObjects {
		t.Fatalf("test invariant broken: n=%d exceeds the group buffer cap %d", n, rawReplayMaxGroupObjects)
	}

	r := NewRelay()
	r.EnsureRawTrack("video", ReplayCurrentGroup)
	v := newMockRawViewer("joiner")

	var wg sync.WaitGroup
	wg.Add(3)

	go func() { // broadcaster
		defer wg.Done()
		for i := 0; i < n; i++ {
			r.BroadcastObject("video", rawObj(1, uint64(i)))
		}
	}()
	go func() { // joiner, racing the broadcaster
		defer wg.Done()
		r.AddRawViewer("video", v)
	}()
	go func() { // reader path under contention
		defer wg.Done()
		for i := 0; i < n; i++ {
			_ = r.RawViewerCount("video")
		}
	}()

	wg.Wait()

	got := v.objectIDs()
	if len(got) != n {
		t.Fatalf("joiner received %d objects, want %d (complete, gap-free run)", len(got), n)
	}
	for i, id := range got {
		if id != uint64(i) {
			t.Fatalf("joiner object #%d = %d, want %d (in-order, no gap/dup)", i, id, i)
		}
	}
}
