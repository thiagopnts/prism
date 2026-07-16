package distribution

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/quic-go/quic-go/quicvarint"
	"github.com/zsiec/ccx"
	"github.com/zsiec/prism/media"
	"github.com/zsiec/prism/moq"
)

// mergeSetupTimeout bounds how long the merge run goroutine waits for each
// source (the anchor and every external) to make its catalog available before
// giving up on that source. It is deliberately shorter than videoInfoTimeout so
// a slow/unreachable external does not exhaust a downstream viewer's own wait
// budget in handleMoQ.
const mergeSetupTimeout = 15 * time.Second

// mergeMaxBackoff caps the wait between merge setup attempts when a source is not
// yet ready (mirrors RelayPuller's reconnect backoff: 1s → mergeMaxBackoff).
const mergeMaxBackoff = 30 * time.Second

// mergeExternal binds an external feed key to the relay it is pulled through.
type mergeExternal struct {
	key   string
	relay *Relay
}

// resolvedExternalTrack is the single media track discovered in an external
// feed's catalog, carried into the merged catalog under the external's key.
type resolvedExternalTrack struct {
	key             string
	sourceTrackName string
	selectionParams moqSelectionParams
}

// MergePuller drives a synthetic "merge" relay that re-serves an anchor stream's
// tracks plus one track per external feed, all under the anchor's namespace, so a
// consumer subscribing to the anchor stream key transparently receives the
// external caption feeds too. It implements upstreamPuller, so a fresh NewRelay()
// with a MergePuller attached presents as a relayed stream to the rest of the
// codebase (isRelayed() == true) and every track is served through the verbatim
// raw path (see relayed.go / handleRawSubscribe).
//
// The anchor is the timing reference; external tracks are forwarded verbatim for
// now. Per-source timestamp realignment against the anchor timeline is deferred —
// the forwarding point (BroadcastObject / the origin tap's frame encode) is where
// it will slot in.
//
// Unlike RelayPuller it holds no upstream connection of its own: it taps the
// anchor and external relays (which own their own connections) and forwards their
// objects into the merge relay. All acquisition happens once in run(), gated on
// the whole merge relay's viewer presence via EnsureStarted/Stop; the per-track
// Subscribe/Unsubscribe hooks are intentional no-ops (fixed track set — every
// consumer sees the same merged catalog).
type MergePuller struct {
	merge     *Relay
	id        string
	anchorKey string
	anchor    *Relay
	externals []mergeExternal
	log       *slog.Logger

	// tap forwards an origin anchor's media frames into the merge relay. Unused
	// when the anchor is itself relayed (its raw tracks are tapped directly).
	tap *originAnchorTap

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	// done is closed when the current run goroutine has fully exited (including
	// its deferred tap teardown). Stop waits on it so a restart's setup can never
	// overlap the previous run's teardown — critical because tap/forwarder viewer
	// IDs are deterministic and identical across runs, so an overlapping teardown
	// would delete the freshly-installed registrations. nil while not running.
	done chan struct{}
}

var _ upstreamPuller = (*MergePuller)(nil)

// NewMergePuller creates a puller bound to merge. id must be unique per merge
// relay (the server passes the internal cache key); it prefixes the tap and
// forwarder viewer IDs so multiple merge relays tapping the same source relay do
// not collide in that source's viewer maps.
func NewMergePuller(merge *Relay, id, anchorKey string, anchor *Relay, externals []mergeExternal) *MergePuller {
	mp := &MergePuller{
		merge:     merge,
		id:        id,
		anchorKey: anchorKey,
		anchor:    anchor,
		externals: externals,
		log:       slog.With("component", "merge_puller", "anchor", anchorKey),
	}
	mp.tap = &originAnchorTap{
		id:    id + ":anchortap",
		merge: merge,
		audio: make(map[int]*audioCounter),
	}
	return mp
}

func (mp *MergePuller) forwarderID(name string) string { return mp.id + ":" + name }

// Subscribe / Unsubscribe (trackPuller) are no-ops: the merge relay exposes a
// fixed merged track set acquired once in run(), not negotiated per downstream
// track. The merge relay's own per-track refcount/grace machinery still runs for
// its downstream viewers; it just drives these no-ops.
func (mp *MergePuller) Subscribe(string)   {}
func (mp *MergePuller) Unsubscribe(string) {}

// EnsureStarted launches the merge lifecycle once, deriving its context from ctx
// (the merge relay's upstream parent context). Idempotent while running;
// restart-capable after Stop. It returns immediately — all blocking work (waiting
// on source catalogs, dialing externals) happens in run() so this never blocks
// EnsureUpstream under the relay's trackMu.
func (mp *MergePuller) EnsureStarted(ctx context.Context) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	if mp.started {
		return
	}
	mp.started = true
	runCtx, cancel := context.WithCancel(ctx)
	mp.cancel = cancel
	done := make(chan struct{})
	mp.done = done
	go func() {
		defer close(done)
		mp.run(runCtx)
	}()
}

// Stop cancels the merge lifecycle and resets so a later EnsureStarted can
// restart it (e.g. when viewers return after the idle grace released the taps).
// It is called from the relay's onUpstreamIdle under the merge relay's trackMu, so
// it must not take any server-level lock: it only cancels the run context. It then
// waits for the run goroutine to finish tearing the taps down, so the next
// EnsureStarted (serialised after this Stop by the same trackMu) cannot start a new
// run that races the old teardown over the shared, deterministic viewer IDs. The
// wait is safe under trackMu because run's teardown only touches the anchor/external
// relays' locks, never this merge relay's trackMu.
func (mp *MergePuller) Stop() {
	mp.mu.Lock()
	cancel := mp.cancel
	done := mp.done
	mp.cancel = nil
	mp.done = nil
	mp.started = false
	mp.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// run keeps trying to establish the merge until it succeeds or ctx is cancelled,
// backing off between failed attempts (a source's catalog may not be ready yet —
// e.g. a relayed anchor whose upstream is still coming up). Once an attempt
// publishes the merged catalog it blocks there until ctx is cancelled, so run
// returns only on teardown. This mirrors RelayPuller's reconnect loop and ensures
// the catalog is eventually published rather than the relay wedging on a single
// early failure.
func (mp *MergePuller) run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		if mp.attempt(ctx) {
			return
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff *= 2
		if backoff > mergeMaxBackoff {
			backoff = mergeMaxBackoff
		}
	}
}

// attempt performs one setup try. It returns true once it has published the merged
// catalog and then blocked until ctx was cancelled (run should stop); it returns
// false if setup failed before publishing (run should back off and retry). Its
// deferred cleanup undoes exactly what this attempt registered, so both a failed
// attempt (before retry) and a cancelled published attempt unwind fully.
func (mp *MergePuller) attempt(ctx context.Context) bool {
	relayed := mp.anchor.isRelayed()

	type extHold struct {
		relay       *Relay
		sourceName  string // "" until the media track is acquired
		forwarderID string
	}
	var (
		anchorUpstreamHeld bool
		anchorTracks       []string
		anchorForwarders   = map[string]string{}
		originTapAdded     bool
		extHolds           []extHold
	)

	defer func() {
		for i := len(extHolds) - 1; i >= 0; i-- {
			h := extHolds[i]
			if h.sourceName != "" {
				h.relay.ReleaseTrack(h.sourceName)
				h.relay.RemoveRawViewer(h.sourceName, h.forwarderID)
			}
			h.relay.ReleaseUpstream()
		}
		if relayed {
			for _, name := range anchorTracks {
				mp.anchor.ReleaseTrack(name)
				mp.anchor.RemoveRawViewer(name, anchorForwarders[name])
			}
			if anchorUpstreamHeld {
				mp.anchor.ReleaseUpstream()
			}
		} else if originTapAdded {
			mp.anchor.RemoveViewer(mp.tap.ID())
		}
	}()

	// --- anchor catalog ---
	var anchorCatalogJSON []byte
	if relayed {
		mp.anchor.EnsureUpstream()
		anchorUpstreamHeld = true
		cctx, cancel := context.WithTimeout(ctx, mergeSetupTimeout)
		ready := mp.anchor.WaitVideoInfo(cctx) && mp.anchor.WaitCatalogReady(cctx)
		cancel()
		if !ready {
			mp.log.Warn("merge: anchor catalog not ready")
			return false
		}
		anchorCatalogJSON = mp.anchor.Catalog()
		if anchorCatalogJSON == nil {
			mp.log.Warn("merge: relayed anchor has no catalog")
			return false
		}
	} else {
		cctx, cancel := context.WithTimeout(ctx, mergeSetupTimeout)
		// Best-effort, matching a normal origin viewer (handleMoQ ignores these):
		// buildMoQCatalog falls back to defaults if the first keyframe never lands.
		mp.anchor.WaitVideoInfo(cctx)
		mp.anchor.WaitCatalogReady(cctx)
		cancel()
		var err error
		anchorCatalogJSON, err = buildMoQCatalog(mp.anchorKey, mp.anchor, false)
		if err != nil {
			mp.log.Warn("merge: build anchor catalog failed", "error", err)
			return false
		}
	}
	if ctx.Err() != nil {
		return false
	}

	// --- externals: discover single track + forward it under the external key ---
	resolved := make([]resolvedExternalTrack, 0, len(mp.externals))
	for _, e := range mp.externals {
		if ctx.Err() != nil {
			return false
		}
		e.relay.EnsureUpstream()
		extHolds = append(extHolds, extHold{relay: e.relay})
		hi := len(extHolds) - 1

		cctx, cancel := context.WithTimeout(ctx, mergeSetupTimeout)
		ready := e.relay.WaitVideoInfo(cctx) && e.relay.WaitCatalogReady(cctx)
		cancel()
		if !ready {
			mp.log.Warn("merge: external catalog not ready; skipping", "external", e.key)
			continue
		}
		track, ok := discoverExternalTrack(e.relay.Catalog())
		if !ok {
			mp.log.Warn("merge: external has no usable track; skipping", "external", e.key)
			continue
		}

		mp.merge.EnsureRawTrack(e.key, replayPolicyForTrack(track.Name))
		fwID := mp.forwarderID(e.key)
		e.relay.AcquireTrack(track.Name)
		e.relay.AddRawViewer(track.Name, &rawForwarder{id: fwID, target: e.key, merge: mp.merge})
		extHolds[hi].sourceName = track.Name
		extHolds[hi].forwarderID = fwID

		resolved = append(resolved, resolvedExternalTrack{
			key:             e.key,
			sourceTrackName: track.Name,
			selectionParams: track.SelectionParams,
		})
	}
	if ctx.Err() != nil {
		return false
	}

	// --- anchor taps ---
	if relayed {
		for _, name := range relayableTrackNames(anchorCatalogJSON) {
			mp.merge.EnsureRawTrack(name, replayPolicyForTrack(name))
			fwID := mp.forwarderID(name)
			mp.anchor.AcquireTrack(name)
			mp.anchor.AddRawViewer(name, &rawForwarder{id: fwID, target: name, merge: mp.merge})
			anchorTracks = append(anchorTracks, name)
			anchorForwarders[name] = fwID
		}
	} else {
		mp.tap.reset()
		mp.merge.EnsureRawTrack("video", ReplayCurrentGroup)
		mp.merge.EnsureRawTrack("captions", ReplayCurrentGroup)
		for _, at := range mp.anchor.ObservedAudioTracks() {
			mp.merge.EnsureRawTrack(fmt.Sprintf("audio%d", at.Index), ReplayRecentRing)
		}
		mp.anchor.AddViewer(mp.tap)
		originTapAdded = true
	}

	// --- publish the merged catalog (unblocks downstream viewers) ---
	mergedJSON, err := buildMergedCatalog(mp.anchorKey, anchorCatalogJSON, resolved)
	if err != nil {
		mp.log.Warn("merge: build merged catalog failed", "error", err)
		return false
	}
	mp.merge.SetCatalog(mergedJSON)
	mp.log.Info("merge relay ready", "externals", len(resolved), "relayedAnchor", relayed)

	<-ctx.Done()
	return true
}

// relayableTrackNames returns the media track names in a verbatim catalog that a
// relayed anchor forwards (all tracks except the server-synthesized stats/control
// tracks, which have no generic raw object source).
func relayableTrackNames(catalogJSON []byte) []string {
	var c moqCatalog
	if err := json.Unmarshal(catalogJSON, &c); err != nil {
		return nil
	}
	var names []string
	for _, t := range c.Tracks {
		if t.Name == "stats" || t.Name == "control" {
			continue
		}
		names = append(names, t.Name)
	}
	return names
}

// discoverExternalTrack picks the single media track an external feed carries
// (per the current caption-only use case). stats/control are skipped; the first
// remaining track wins.
func discoverExternalTrack(catalogJSON []byte) (moqCatalogTrack, bool) {
	if catalogJSON == nil {
		return moqCatalogTrack{}, false
	}
	var c moqCatalog
	if err := json.Unmarshal(catalogJSON, &c); err != nil {
		return moqCatalogTrack{}, false
	}
	for _, t := range c.Tracks {
		if t.Name == "stats" || t.Name == "control" {
			continue
		}
		return t, true
	}
	return moqCatalogTrack{}, false
}

// buildMergedCatalog synthesizes the merged catalog served to consumers: the
// anchor's catalog (minus stats/control) with one track appended per external,
// named after the external key and carrying that external's selection params
// verbatim. The namespace is forced to the anchor's wire-visible stream key so an
// unmodified consumer subscribes every track under the one namespace it knows.
func buildMergedCatalog(streamKey string, anchorCatalogJSON []byte, externals []resolvedExternalTrack) ([]byte, error) {
	var c moqCatalog
	if err := json.Unmarshal(anchorCatalogJSON, &c); err != nil {
		return nil, fmt.Errorf("parse anchor catalog: %w", err)
	}

	tracks := make([]moqCatalogTrack, 0, len(c.Tracks)+len(externals))
	for _, t := range c.Tracks {
		if t.Name == "stats" || t.Name == "control" {
			continue
		}
		tracks = append(tracks, t)
	}
	for _, e := range externals {
		tracks = append(tracks, moqCatalogTrack{Name: e.key, SelectionParams: e.selectionParams})
	}

	c.CommonTrackFields.Namespace = fmt.Sprintf("prism/%s", streamKey)
	c.Tracks = tracks
	return json.Marshal(c)
}

// rawForwarder is a RawViewer that forwards every verbatim object it receives on
// a source relay's track into the merge relay under a (possibly different) target
// track name. Used to tap a relayed anchor's tracks and each external's track.
type rawForwarder struct {
	id     string
	target string
	merge  *Relay
}

var _ RawViewer = (*rawForwarder)(nil)

func (f *rawForwarder) ID() string               { return f.id }
func (f *rawForwarder) SendObject(obj RawObject) { f.merge.BroadcastObject(f.target, obj) }

// audioCounter tracks per-audio-track object numbering for the origin tap.
type audioCounter struct {
	started  bool
	objectID uint64
}

// originAnchorTap is a Viewer registered on an origin anchor relay; it re-encodes
// each media frame into a RawObject byte-identical to what the origin write path
// (moqWriter) would emit, and forwards it into the merge relay's raw track. This
// lets an origin stream feed the verbatim raw fan-out the merge relay serves.
//
// Object numbering mirrors moqWriter exactly (see moq_writer.go / the write loops
// in moq_session.go): video resets the object ID per GOP (keyframe opens a new
// subgroup stream), audio rides one boundary-less stream, each caption is its own
// single-object group.
type originAnchorTap struct {
	id    string
	merge *Relay

	// mu serialises counter updates with the BroadcastObject they gate so
	// per-track object ordering holds even if the anchor broadcasts media types
	// from different goroutines. Lock order is tap.mu → relay.rawMu (BroadcastObject
	// / EnsureRawTrack); nothing takes tap.mu under rawMu, so it is acyclic.
	mu            sync.Mutex
	videoStarted  bool
	videoGroupID  uint64
	videoObjectID uint64
	captionGroup  uint64
	audio         map[int]*audioCounter
}

var _ Viewer = (*originAnchorTap)(nil)

func (t *originAnchorTap) ID() string         { return t.id }
func (t *originAnchorTap) Stats() ViewerStats { return ViewerStats{ID: t.id} }

// reset clears object numbering so a restart (EnsureStarted after Stop) begins a
// fresh sequence; the GOP replay AddViewer performs starts on a keyframe.
func (t *originAnchorTap) reset() {
	t.mu.Lock()
	t.videoStarted = false
	t.videoGroupID = 0
	t.videoObjectID = 0
	t.captionGroup = 0
	t.audio = make(map[int]*audioCounter)
	t.mu.Unlock()
}

func (t *originAnchorTap) SendVideo(frame *media.VideoFrame) {
	// WireData is set once by BroadcastVideo and, for a cache-backed origin feed,
	// is a fresh per-frame allocation the replay buffer can safely retain.
	payload := frame.WireData
	if payload == nil {
		payload = moq.AnnexBToAVC1(frame.NALUs)
	}
	exts := buildVideoExts(frame)

	t.mu.Lock()
	defer t.mu.Unlock()
	if frame.IsKeyframe {
		t.videoGroupID = uint64(frame.GroupID)
		t.videoObjectID = 0
		t.videoStarted = true
	} else if !t.videoStarted {
		return // mirror writeVideoLoop: drop deltas until the first keyframe
	}
	obj := RawObject{
		GroupID:      t.videoGroupID,
		ObjectID:     t.videoObjectID,
		Priority:     priorityVideo,
		ExtBytes:     exts,
		Payload:      payload,
		StartsStream: frame.IsKeyframe,
	}
	t.videoObjectID++
	t.merge.BroadcastObject("video", obj)
}

func (t *originAnchorTap) SendAudio(frame *media.AudioFrame) {
	payload := moq.StripADTS(frame.Data)
	tsMS := uint32(frame.PTS / 1000)
	var exts []byte
	exts = quicvarint.Append(exts, locExtCaptureTimestamp)
	exts = quicvarint.Append(exts, uint64(tsMS)*1000)
	name := fmt.Sprintf("audio%d", frame.TrackIndex)

	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.audio[frame.TrackIndex]
	if c == nil {
		c = &audioCounter{}
		t.audio[frame.TrackIndex] = c
		t.merge.EnsureRawTrack(name, ReplayRecentRing)
	}
	startsStream := !c.started
	c.started = true
	obj := RawObject{
		GroupID:      0,
		ObjectID:     c.objectID,
		Priority:     priorityAudio,
		ExtBytes:     exts,
		Payload:      payload,
		StartsStream: startsStream,
	}
	c.objectID++
	t.merge.BroadcastObject(name, obj)
}

func (t *originAnchorTap) SendCaptions(frame *ccx.CaptionFrame) {
	data := frame.Serialize()
	tsMS := uint32(frame.PTS / 1000)
	var exts []byte
	exts = quicvarint.Append(exts, locExtCaptureTimestamp)
	exts = quicvarint.Append(exts, uint64(tsMS)*1000)

	t.mu.Lock()
	defer t.mu.Unlock()
	obj := RawObject{
		GroupID:      t.captionGroup,
		ObjectID:     0,
		Priority:     priorityCaptions,
		ExtBytes:     exts,
		Payload:      data,
		StartsStream: true, // each caption is its own single-object group
	}
	t.captionGroup++
	t.merge.BroadcastObject("captions", obj)
}

// buildVideoExts constructs the LOC extension block for a video frame identically
// to moqWriter.WriteVideoFrame (capture timestamp, RFC 9626 frame marking, and the
// decoder config on keyframes) so the forwarded RawObject is byte-identical to the
// origin write path.
func buildVideoExts(frame *media.VideoFrame) []byte {
	var exts []byte
	exts = quicvarint.Append(exts, locExtCaptureTimestamp)
	exts = quicvarint.Append(exts, uint64(frame.PTS))

	exts = quicvarint.Append(exts, locExtVideoFrameMarking)
	if frame.IsKeyframe {
		exts = quicvarint.Append(exts, vfmKeyframe)
	} else {
		exts = quicvarint.Append(exts, vfmNonKeyframe)
	}

	if frame.IsKeyframe && frame.SPS != nil && frame.PPS != nil {
		var cfg []byte
		if frame.Codec == "h265" && frame.VPS != nil {
			cfg = moq.BuildHEVCDecoderConfig(frame.VPS, frame.SPS, frame.PPS)
		} else {
			cfg = moq.BuildAVCDecoderConfig(frame.SPS, frame.PPS)
		}
		if cfg != nil {
			exts = quicvarint.Append(exts, locExtVideoConfig)
			exts = quicvarint.Append(exts, uint64(len(cfg)))
			exts = append(exts, cfg...)
		}
	}
	return exts
}
