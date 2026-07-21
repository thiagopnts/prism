package distribution

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/quic-go/quic-go/quicvarint"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestAnchorClock(t *testing.T) {
	t.Parallel()
	var ac anchorClock
	if _, ok := ac.NowPTS(); ok {
		t.Fatal("NowPTS ok=true before any update")
	}
	ac.update(0, 0)  // non-positive ignored
	ac.update(-5, 0) // non-positive ignored
	if _, ok := ac.NowPTS(); ok {
		t.Fatal("NowPTS ok=true after only non-positive updates")
	}
	ac.update(1_000, 0)
	pts, ok := ac.NowPTS()
	if !ok || pts != 1_000 {
		t.Fatalf("NowPTS = (%d,%v), want (1000,true)", pts, ok)
	}
	ac.update(2_500, 0)
	if pts, _ := ac.NowPTS(); pts != 2_500 {
		t.Fatalf("NowPTS = %d after second update, want 2500", pts)
	}
}

func TestAnchorClockWallToPTS(t *testing.T) {
	t.Parallel()
	var ac anchorClock

	// No timecode correspondence yet → WallToPTS unavailable.
	if _, ok := ac.WallToPTS(1_000); ok {
		t.Fatal("WallToPTS ok=true before any timecode-bearing update")
	}

	// A media PTS with no capture wall (captureWallUS=0) drives NowPTS but must not
	// establish an offset.
	ac.update(5_000_000, 0)
	if _, ok := ac.WallToPTS(1_000); ok {
		t.Fatal("WallToPTS ok=true after update without a capture wall clock")
	}

	// A frame whose media PTS is 5_000_000 and whose real capture wall time is
	// 1_700_000_000_000_000 establishes offset C = pts - wall. WallToPTS(w) = w + C,
	// i.e. it maps that same wall time back to the media PTS.
	const wall = int64(1_700_000_000_000_000)
	const mediaPTS = int64(5_000_000)
	ac.update(mediaPTS, wall)
	got, ok := ac.WallToPTS(wall)
	if !ok || got != mediaPTS {
		t.Fatalf("WallToPTS(capture wall) = (%d,%v), want (%d,true)", got, ok, mediaPTS)
	}
	// A wall time 2s later maps 2s later in the media timebase (slope 1).
	if got, _ := ac.WallToPTS(wall + 2_000_000); got != mediaPTS+2_000_000 {
		t.Fatalf("WallToPTS(+2s) = %d, want %d", got, mediaPTS+2_000_000)
	}
}

func TestCaptureTimestampFromExt(t *testing.T) {
	t.Parallel()

	// Build an ext block: video frame marking (id 4, even), then capture ts
	// (id 2, even), then a video config (id 13, odd, length-prefixed) after it to
	// ensure odd extensions are skipped when scanning.
	var ext []byte
	ext = quicvarint.Append(ext, locExtVideoFrameMarking)
	ext = quicvarint.Append(ext, vfmKeyframe)
	ext = quicvarint.Append(ext, locExtCaptureTimestamp)
	ext = quicvarint.Append(ext, 123_456)

	ts, ok := captureTimestampFromExt(ext)
	if !ok || ts != 123_456 {
		t.Fatalf("captureTimestampFromExt = (%d,%v), want (123456,true)", ts, ok)
	}

	// Capture ts appearing after an odd (length-prefixed) extension.
	var ext2 []byte
	ext2 = quicvarint.Append(ext2, locExtVideoConfig)
	ext2 = quicvarint.Append(ext2, 3)
	ext2 = append(ext2, 0xAA, 0xBB, 0xCC)
	ext2 = quicvarint.Append(ext2, locExtCaptureTimestamp)
	ext2 = quicvarint.Append(ext2, 999)
	if ts, ok := captureTimestampFromExt(ext2); !ok || ts != 999 {
		t.Fatalf("captureTimestampFromExt (after odd ext) = (%d,%v), want (999,true)", ts, ok)
	}

	// Absent capture ts → ok=false.
	var ext3 []byte
	ext3 = quicvarint.Append(ext3, locExtVideoFrameMarking)
	ext3 = quicvarint.Append(ext3, vfmNonKeyframe)
	if _, ok := captureTimestampFromExt(ext3); ok {
		t.Fatal("captureTimestampFromExt ok=true with no capture ts present")
	}

	// Empty ext → ok=false.
	if _, ok := captureTimestampFromExt(nil); ok {
		t.Fatal("captureTimestampFromExt ok=true for empty ext")
	}
}

func TestAnchorTimingFromExt(t *testing.T) {
	t.Parallel()

	// Full ext block as the video write path emits it: capture ts (id 2), frame
	// marking (id 4), capture wall clock (id 8), then a config (id 13, odd).
	var ext []byte
	ext = quicvarint.Append(ext, locExtCaptureTimestamp)
	ext = quicvarint.Append(ext, 5_000_000)
	ext = quicvarint.Append(ext, locExtVideoFrameMarking)
	ext = quicvarint.Append(ext, vfmKeyframe)
	ext = quicvarint.Append(ext, locExtCaptureWallClock)
	ext = quicvarint.Append(ext, 1_700_000_000_000_000)
	ext = quicvarint.Append(ext, locExtVideoConfig)
	ext = quicvarint.Append(ext, 2)
	ext = append(ext, 0xAA, 0xBB)

	pts, ptsOK, wall, wallOK := anchorTimingFromExt(ext)
	if !ptsOK || pts != 5_000_000 {
		t.Fatalf("pts = (%d,%v), want (5000000,true)", pts, ptsOK)
	}
	if !wallOK || wall != 1_700_000_000_000_000 {
		t.Fatalf("wall = (%d,%v), want (1700000000000000,true)", wall, wallOK)
	}

	// A block without the wall-clock ext yields wallOK=false but still parses pts.
	var ext2 []byte
	ext2 = quicvarint.Append(ext2, locExtCaptureTimestamp)
	ext2 = quicvarint.Append(ext2, 42)
	if pts, ptsOK, _, wallOK := anchorTimingFromExt(ext2); !ptsOK || pts != 42 || wallOK {
		t.Fatalf("anchorTimingFromExt(no wall) = (pts=%d,%v, wallOK=%v), want (42,true,false)", pts, ptsOK, wallOK)
	}
}

// newPacerHarness wires a merge relay + raw track + recording viewer + anchor
// clock + pacer for deterministic drain() tests (no goroutine/ticker).
func newPacerHarness(t *testing.T, trackName string) (*virtualPacer, *anchorClock, *mockRawViewer) {
	t.Helper()
	merge := NewRelay()
	merge.EnsureRawTrack(trackName, ReplayCurrentGroup)
	v := newMockRawViewer("viewer")
	merge.AddRawViewer(trackName, v)
	ac := &anchorClock{}
	p := newVirtualPacer(merge, trackName, ac, testLogger())
	return p, ac, v
}

func TestVirtualPacerNoAnchorNoEmit(t *testing.T) {
	t.Parallel()
	p, _, v := newPacerHarness(t, "nlc-x-es")
	p.Push(1_000, []byte("a"))
	p.drain() // anchor clock has no reading yet
	if v.count() != 0 {
		t.Fatalf("emitted %d objects before anchor clock set, want 0", v.count())
	}
}

func TestVirtualPacerReleasesDueHoldsAhead(t *testing.T) {
	t.Parallel()
	p, ac, v := newPacerHarness(t, "nlc-x-es")

	// Three frames: two at/under the anchor, one ahead.
	p.Push(1_000, []byte("f1"))
	p.Push(2_000, []byte("f2"))
	p.Push(9_000, []byte("f3-ahead"))

	ac.update(2_000, 0)
	p.drain()

	// f1 and f2 are due; f3 is held.
	if v.count() != 2 {
		t.Fatalf("emitted %d objects, want 2 (f1,f2)", v.count())
	}
	got := v.got
	if string(got[0].Payload) != "f1" || string(got[1].Payload) != "f2" {
		t.Fatalf("payloads = %q,%q; want f1,f2", got[0].Payload, got[1].Payload)
	}
	// Each caption is its own single-object group with incrementing GroupID.
	if got[0].GroupID != 0 || got[1].GroupID != 1 {
		t.Fatalf("group IDs = %d,%d; want 0,1", got[0].GroupID, got[1].GroupID)
	}
	if !got[0].StartsStream || got[0].ObjectID != 0 || got[0].Priority != priorityCaptions {
		t.Fatalf("obj0 framing wrong: starts=%v id=%d prio=%d", got[0].StartsStream, got[0].ObjectID, got[0].Priority)
	}
	// Capture timestamp extension equals the frame PTS.
	if ts, ok := captureTimestampFromExt(got[1].ExtBytes); !ok || ts != 2_000 {
		t.Fatalf("f2 capture ts = (%d,%v), want (2000,true)", ts, ok)
	}

	// Advance the anchor past f3 → it releases now.
	ac.update(9_500, 0)
	p.drain()
	if v.count() != 3 {
		t.Fatalf("emitted %d objects after advancing, want 3", v.count())
	}
	if string(v.got[2].Payload) != "f3-ahead" || v.got[2].GroupID != 2 {
		t.Fatalf("f3 = %q group %d; want f3-ahead group 2", v.got[2].Payload, v.got[2].GroupID)
	}
}

func TestVirtualPacerDropsBehind(t *testing.T) {
	t.Parallel()
	p, ac, v := newPacerHarness(t, "nlc-x-es")

	now := int64(5_000_000) // 5s anchor position
	// A frame more than the behind threshold older than the anchor is dropped.
	p.Push(now-virtualPaceBehindUS-1, []byte("stale"))
	// A frame within the behind threshold (still older than now) is emitted late.
	p.Push(now-virtualPaceBehindUS+1, []byte("late-ok"))

	ac.update(now, 0)
	p.drain()

	if v.count() != 1 {
		t.Fatalf("emitted %d objects, want 1 (only late-ok)", v.count())
	}
	if string(v.got[0].Payload) != "late-ok" {
		t.Fatalf("emitted %q, want late-ok", v.got[0].Payload)
	}
}

func TestVirtualPacerBufferHorizon(t *testing.T) {
	t.Parallel()
	p, _, _ := newPacerHarness(t, "nlc-x-es")
	for i := 0; i < virtualPaceMaxBuffer+50; i++ {
		p.Push(int64(i+1)*1_000_000, []byte("x"))
	}
	p.mu.Lock()
	n := len(p.buf)
	p.mu.Unlock()
	if n != virtualPaceMaxBuffer {
		t.Fatalf("buffer len = %d, want capped at %d", n, virtualPaceMaxBuffer)
	}
}

func TestVirtualPacerPushSortsOutOfOrder(t *testing.T) {
	t.Parallel()
	p, ac, v := newPacerHarness(t, "nlc-x-es")
	// Push out of order; drain must still emit in PTS order.
	p.Push(3_000, []byte("c"))
	p.Push(1_000, []byte("a"))
	p.Push(2_000, []byte("b"))
	ac.update(3_000, 0)
	p.drain()
	if v.count() != 3 {
		t.Fatalf("emitted %d, want 3", v.count())
	}
	if string(v.got[0].Payload) != "a" || string(v.got[1].Payload) != "b" || string(v.got[2].Payload) != "c" {
		t.Fatalf("order = %q,%q,%q; want a,b,c", v.got[0].Payload, v.got[1].Payload, v.got[2].Payload)
	}
}

func TestBuildMergedCatalogVirtualTrack(t *testing.T) {
	t.Parallel()
	anchor := NewRelay()
	anchorCat, err := buildMoQCatalog("anchorkey", anchor, false)
	if err != nil {
		t.Fatal(err)
	}

	externals := []resolvedExternalTrack{{
		key:             "nlc-2306899-es",
		selectionParams: SelectionParams{Codec: "caption/v2"},
	}}
	merged, err := buildMergedCatalog("anchorkey", anchorCat, externals)
	if err != nil {
		t.Fatal(err)
	}

	var cat moqCatalog
	if err := json.Unmarshal(merged, &cat); err != nil {
		t.Fatal(err)
	}
	// Namespace forced to the anchor's wire key.
	if cat.CommonTrackFields.Namespace != "prism/anchorkey" {
		t.Fatalf("namespace = %q, want prism/anchorkey", cat.CommonTrackFields.Namespace)
	}
	// stats/control dropped; the virtual track appended with its selection params.
	var found *moqCatalogTrack
	for i := range cat.Tracks {
		if cat.Tracks[i].Name == "nlc-2306899-es" {
			found = &cat.Tracks[i]
		}
		if cat.Tracks[i].Name == "stats" || cat.Tracks[i].Name == "control" {
			t.Fatalf("merged catalog still carries %q", cat.Tracks[i].Name)
		}
	}
	if found == nil {
		t.Fatal("merged catalog missing nlc-2306899-es track")
	}
	if found.SelectionParams.Codec != "caption/v2" {
		t.Fatalf("virtual track codec = %q, want caption/v2", found.SelectionParams.Codec)
	}
}
