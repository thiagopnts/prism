package distribution

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go/quicvarint"
)

// This file holds the generic "virtual external" surface: a caller-assembled
// external track (e.g. captions built from an HTTP feed) merged into an anchor
// stream exactly like a MoQ external, minus the upstream pull. All heavy logic —
// fetching, encoding, epoch/timing math — lives in the caller (the library that
// embeds prism). prism only drives the source lifecycle, owns the MoQ framing
// (LOC capture-timestamp extension, per-object group numbering), and paces the
// caller's frames against the anchor timeline.

// SelectionParams is the exported form of the catalog track's selection params,
// used by a VirtualExternalSource to describe its track in the merged catalog
// (e.g. SelectionParams{Codec: "caption/v2"}).
type SelectionParams = moqSelectionParams

// VirtualExternalSource is a caller-assembled external feed. It is the non-MoQ
// analogue of the relayed external in MergePuller: instead of tapping an upstream
// relay's track, prism starts the source and forwards the frames it pushes.
//
// Lifecycle is driven by prism: Start is called once when the merge relay begins
// (a viewer is present), Stop when it tears down. A source must be restart-safe —
// a later Start may follow a Stop when viewers return after the idle grace.
type VirtualExternalSource interface {
	// TrackName is the track's key in both the merged catalog and the raw
	// fan-out (e.g. "nlc-2306899-es"). Stable across Start/Stop cycles.
	TrackName() string
	// SelectionParams describes the track in the merged catalog. For a caption
	// feed this is SelectionParams{Codec: "caption/v2"} so a consumer routes it
	// as a caption track.
	SelectionParams() SelectionParams
	// Start begins producing frames until ctx is cancelled. The source pushes
	// each assembled frame into sink with a target PTS in the anchor's
	// capture-timestamp timebase (microseconds); anchor exposes the anchor's
	// current position so the source can map its own timeline (e.g. a wall-clock
	// epoch) into that timebase. Start must not block: spawn a goroutine.
	Start(ctx context.Context, sink VirtualFrameSink, anchor AnchorClock)
	// Stop halts production started by Start. Idempotent.
	Stop()
}

// VirtualFrameSink receives assembled frames from a VirtualExternalSource. Push
// hands prism a fully-encoded payload with its target PTS (microseconds, anchor
// timebase); prism buffers it and releases it into the merge relay's raw track
// when the anchor reaches that PTS. Push never blocks and never fails — an
// over-horizon frame is dropped with a warning.
type VirtualFrameSink interface {
	Push(pts int64, payload []byte)
}

// AnchorClock exposes the anchor stream's timing so a source can map its own
// timeline into the anchor's. ok is false until the required anchor data has been
// observed.
type AnchorClock interface {
	// NowPTS returns the anchor's latest capture timestamp (media-clock
	// microseconds), or ok=false until the first anchor frame is seen. Used to gate
	// release of buffered frames against the live edge.
	NowPTS() (pts int64, ok bool)
	// WallToPTS maps a UTC wall-clock time (microseconds since the Unix epoch) into
	// the anchor's media-PTS timebase, using the media-PTS↔UTC correspondence
	// carried by the anchor's embedded wall-clock timecode. ok=false until such a
	// correspondence has been observed (no timecode yet, or the stream carries
	// none), so callers can fall back. This mapping is latency-free: the
	// correspondence comes from a frame's own capture timecode, not from "now", so
	// it is not skewed by the anchor's capture-to-ingest latency.
	WallToPTS(wallUS int64) (pts int64, ok bool)
}

// anchorClock is the concrete AnchorClock: the most recent capture timestamp seen
// on the anchor's video path (origin tap or relayed forwarder), plus the learned
// media-PTS↔UTC offset. Updated from the merge data path, read by virtual
// sources/pacers, so it is lock-free.
type anchorClock struct {
	pts atomic.Int64
	set atomic.Bool

	// wallOffsetUS is the anchor's (mediaPTS − captureWallUTC) constant in
	// microseconds, learned from frames carrying a wall-clock timecode. Adding it to
	// a UTC wall time yields the matching anchor media PTS. offsetSet guards it.
	wallOffsetUS atomic.Int64
	offsetSet    atomic.Bool
}

// update records the anchor's latest capture timestamp (media-clock microseconds)
// and, when the frame carries a real capture wall time (captureWallUS > 0, UTC
// microseconds reconstructed from an embedded timecode), refreshes the
// media-PTS↔UTC offset. Non-positive pts is ignored (an object with no/zero
// capture timestamp carries no timing).
func (a *anchorClock) update(pts, captureWallUS int64) {
	if pts <= 0 {
		return
	}
	a.pts.Store(pts)
	a.set.Store(true)
	if captureWallUS > 0 {
		a.wallOffsetUS.Store(pts - captureWallUS)
		a.offsetSet.Store(true)
	}
}

// NowPTS returns the anchor's latest capture timestamp, or ok=false if none has
// been observed yet.
func (a *anchorClock) NowPTS() (int64, bool) {
	if !a.set.Load() {
		return 0, false
	}
	return a.pts.Load(), true
}

// WallToPTS maps a UTC wall time (µs) into the anchor's media-PTS timebase using
// the learned offset. ok=false until a timecode-bearing frame has been observed.
func (a *anchorClock) WallToPTS(wallUS int64) (int64, bool) {
	if !a.offsetSet.Load() {
		return 0, false
	}
	return wallUS + a.wallOffsetUS.Load(), true
}

var _ AnchorClock = (*anchorClock)(nil)

// Pacing tunables. Frame PTS and the anchor clock are both microseconds.
const (
	// virtualPaceInterval is how often the pacer re-evaluates the buffer against
	// the anchor clock. Fine enough for smooth caption delivery, coarse enough to
	// be negligible overhead.
	virtualPaceInterval = 40 * time.Millisecond
	// virtualPaceBehindUS: a due frame whose PTS trails the anchor by more than
	// this is dropped rather than emitted late (drop-behind).
	virtualPaceBehindUS = int64(2 * time.Second / time.Microsecond)
	// virtualPaceAheadWarnUS: when the earliest buffered frame leads the anchor by
	// more than this, the source timeline is running ahead of the anchor; warn
	// (throttled) but keep holding the frame (hold-ahead).
	virtualPaceAheadWarnUS = int64(5 * time.Second / time.Microsecond)
	// virtualPaceMaxBuffer bounds the held-frame horizon so a source that races
	// ahead (or an anchor that never advances) cannot grow the buffer without
	// limit. Well above a typical caption buffer depth.
	virtualPaceMaxBuffer = 4096
	// virtualPaceWarnInterval throttles the drop-behind / run-ahead warnings.
	virtualPaceWarnInterval = 5 * time.Second
)

// virtualFrame is a single buffered frame awaiting its anchor-aligned release.
type virtualFrame struct {
	pts     int64
	payload []byte
}

// virtualPacer implements VirtualFrameSink for one virtual track. It buffers
// pushed frames in PTS order and, on each tick, releases the frames now due
// against the anchor clock into the merge relay's raw track — dropping frames too
// far behind and holding frames still ahead. Each released frame is its own
// single-object caption group (mirroring originAnchorTap.SendCaptions), carrying
// a LOC capture-timestamp extension equal to its PTS.
type virtualPacer struct {
	merge     *Relay
	trackName string
	anchor    AnchorClock
	log       *slog.Logger

	mu  sync.Mutex
	buf []virtualFrame

	// groupID numbers released caption groups; only touched by the drain
	// goroutine, so it needs no lock.
	groupID uint64

	lastBehindWarn time.Time
	lastAheadWarn  time.Time
}

var _ VirtualFrameSink = (*virtualPacer)(nil)

func newVirtualPacer(merge *Relay, trackName string, anchor AnchorClock, log *slog.Logger) *virtualPacer {
	return &virtualPacer{
		merge:     merge,
		trackName: trackName,
		anchor:    anchor,
		log:       log.With("track", trackName),
	}
}

// Push inserts a frame into the buffer in PTS order. Over-horizon pushes are
// rejected (the newest frame is dropped) so the held-frame set stays bounded.
func (p *virtualPacer) Push(pts int64, payload []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.buf) >= virtualPaceMaxBuffer {
		p.log.Warn("virtual pacer buffer full; dropping frame", "pts", pts, "buffered", len(p.buf))
		return
	}
	// Most pushes arrive in monotonic PTS order (append); fall back to a sorted
	// insert for the occasional out-of-order frame so drain can assume order.
	i := sort.Search(len(p.buf), func(i int) bool { return p.buf[i].pts > pts })
	p.buf = append(p.buf, virtualFrame{})
	copy(p.buf[i+1:], p.buf[i:])
	p.buf[i] = virtualFrame{pts: pts, payload: payload}
}

// run drives the pacer until ctx is cancelled.
func (p *virtualPacer) run(ctx context.Context) {
	t := time.NewTicker(virtualPaceInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.drain()
		}
	}
}

// drain releases every frame now due against the anchor clock. Frames with
// pts <= anchorNow are emitted (or dropped if behind by more than the threshold);
// frames still ahead are held. No-op until the anchor clock has a reading.
func (p *virtualPacer) drain() {
	now, ok := p.anchor.NowPTS()
	if !ok {
		return
	}

	p.mu.Lock()
	var (
		due     []virtualFrame
		dropped int
	)
	i := 0
	for i < len(p.buf) {
		f := p.buf[i]
		if f.pts > now {
			break // sorted: the rest are still ahead (hold-ahead)
		}
		if f.pts < now-virtualPaceBehindUS {
			dropped++
			i++
			continue
		}
		due = append(due, f)
		i++
	}
	if i > 0 {
		// Drop the consumed prefix without retaining its backing payloads.
		rest := make([]virtualFrame, len(p.buf)-i)
		copy(rest, p.buf[i:])
		p.buf = rest
	}
	var aheadBy int64 = -1
	if len(p.buf) > 0 {
		aheadBy = p.buf[0].pts - now
	}
	p.mu.Unlock()

	for _, f := range due {
		p.emit(f)
	}

	if dropped > 0 {
		if t := time.Now(); t.Sub(p.lastBehindWarn) >= virtualPaceWarnInterval {
			p.lastBehindWarn = t
			p.log.Warn("virtual pacer dropped frames behind anchor", "count", dropped, "behindThresholdUS", virtualPaceBehindUS)
		}
	}
	if aheadBy > virtualPaceAheadWarnUS {
		if t := time.Now(); t.Sub(p.lastAheadWarn) >= virtualPaceWarnInterval {
			p.lastAheadWarn = t
			p.log.Warn("virtual pacer running ahead of anchor", "aheadUS", aheadBy)
		}
	}
}

// emit forwards a single frame into the merge relay's raw track as its own
// single-object caption group with a LOC capture-timestamp extension = its PTS.
func (p *virtualPacer) emit(f virtualFrame) {
	var exts []byte
	exts = quicvarint.Append(exts, locExtCaptureTimestamp)
	exts = quicvarint.Append(exts, uint64(f.pts))

	obj := RawObject{
		GroupID:      p.groupID,
		ObjectID:     0,
		Priority:     priorityCaptions,
		ExtBytes:     exts,
		Payload:      f.payload,
		StartsStream: true, // each caption is its own single-object group
	}
	p.groupID++
	p.merge.BroadcastObject(p.trackName, obj)
}

// anchorTimingFromExt parses the anchor timing extensions out of a verbatim LOC
// extension block: the media capture timestamp (ID 2) and the UTC capture wall
// clock (ID 8), both even IDs carrying a varint microsecond value. The per-value
// ok flags report which were present; a malformed block returns whatever was
// parsed before the error. Odd (length-prefixed) extensions are skipped. Used to
// feed the anchor clock from a relayed anchor's forwarded video objects (which
// carry the extension block byte-for-byte).
func anchorTimingFromExt(ext []byte) (pts int64, ptsOK bool, wallUS int64, wallOK bool) {
	r := bytes.NewReader(ext)
	for r.Len() > 0 {
		id, err := quicvarint.Read(r)
		if err != nil {
			return
		}
		if id%2 == 0 {
			val, err := quicvarint.Read(r)
			if err != nil {
				return
			}
			switch id {
			case locExtCaptureTimestamp:
				pts, ptsOK = int64(val), true
			case locExtCaptureWallClock:
				wallUS, wallOK = int64(val), true
			}
		} else {
			dataLen, err := quicvarint.Read(r)
			if err != nil {
				return
			}
			if uint64(r.Len()) < dataLen {
				return
			}
			if _, err := r.Seek(int64(dataLen), io.SeekCurrent); err != nil {
				return
			}
		}
	}
	return
}

// captureTimestampFromExt returns just the media capture timestamp (LOC extension
// ID 2, microseconds), ok=false if absent or malformed.
func captureTimestampFromExt(ext []byte) (int64, bool) {
	pts, ok, _, _ := anchorTimingFromExt(ext)
	return pts, ok
}
