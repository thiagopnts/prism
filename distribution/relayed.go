package distribution

import (
	"context"
	"time"
)

// This file holds the raw-relay data path: the types and Relay methods used when
// a stream is re-served verbatim from an upstream MoQ publisher rather than
// produced by a local pipeline. The raw path forwards subgroup objects
// byte-for-byte (group/subgroup/object IDs, the extension block, and the
// payload), rewriting only the session-scoped track alias per downstream viewer.
// It is entirely separate from the media path (SendVideo/SendAudio/SendCaptions)
// so origin streams are unaffected.

// RawObject is a single MoQ subgroup object captured verbatim from an upstream
// publisher for byte-for-byte re-serving by a relay.
//
// GroupID, SubgroupID and Priority identify the subgroup stream the object
// belongs to; a downstream writer opens a new uni-stream (and writes a fresh
// subgroup header) whenever these change. ExtBytes is the LOC extension block
// exactly as it appeared on the wire (it is re-prefixed with its varint length
// when written, never re-encoded), so unknown / odd extensions survive the round
// trip. Payload is the object body.
//
// There is deliberately no TrackAlias field: the alias is session-scoped and
// assigned per downstream viewer at write time (see RawStreamWriter).
//
// StartsStream marks the first object of an upstream subgroup stream. A
// downstream writer opens a new uni-stream for it (mirroring the upstream's
// stream boundaries exactly rather than inferring them from GroupID, which is
// ambiguous for audio). It is set by the puller as it begins
// reading each upstream stream and is preserved through the replay buffer, so a
// late joiner replaying a video group sees StartsStream on the keyframe that
// opened the group.
type RawObject struct {
	GroupID      uint64
	SubgroupID   uint64
	ObjectID     uint64
	Priority     byte
	ExtBytes     []byte
	Payload      []byte
	StartsStream bool
}

// RawViewer receives verbatim subgroup objects for a single track. It is the
// raw-path analogue of Viewer; a viewer session implements it (per subscribed
// track) to be fanned out objects the relay pulls from upstream. SendObject must
// not block — implementations push onto a buffered channel and drop on overflow.
type RawViewer interface {
	ID() string
	SendObject(obj RawObject)
}

// RawReplayPolicy selects how a track's recent objects are replayed to a
// late-joining RawViewer so it can begin decoding without waiting for the next
// upstream group boundary.
type RawReplayPolicy int

const (
	// ReplayCurrentGroup replays every object received since the last GroupID
	// change. Correct for tracks whose groups begin with an independently
	// decodable unit — video GOPs that start on a keyframe, caption groups; the
	// upstream is trusted to start each group on such a boundary.
	ReplayCurrentGroup RawReplayPolicy = iota
	// ReplayRecentRing replays a bounded ring of the most recent objects. Correct
	// for a track carried on one long-lived subgroup with no group boundaries —
	// prism writes all audio on groupID=0 forever, so "current group" would grow
	// without bound. AAC is independently decodable, so a short ring is enough to
	// pre-fill a joiner's buffer.
	ReplayRecentRing
	// ReplayNone keeps no replay buffer. Correct for low-rate self-describing
	// tracks (server-synthesized stats), where a joiner simply waits for the next
	// object.
	ReplayNone
)

// rawReplayRingSize bounds ReplayRecentRing buffers (mirrors audioCacheSize:
// ~1s of AAC at ~23ms/frame).
const rawReplayRingSize = 50

// rawReplayMaxGroupObjects bounds a ReplayCurrentGroup buffer so a pathological
// upstream that never changes GroupID cannot grow it without limit (mirrors
// gopCacheMaxFrames).
const rawReplayMaxGroupObjects = 1024

// rawTrack holds the raw-path viewers and replay buffer for a single track.
// Guarded by Relay.rawMu.
type rawTrack struct {
	policy  RawReplayPolicy
	viewers map[string]RawViewer

	// buf is the replay buffer; its meaning depends on policy (current group for
	// ReplayCurrentGroup, recent ring for ReplayRecentRing, unused for ReplayNone).
	buf       []RawObject
	curGroup  uint64
	haveGroup bool
}

func newRawTrack(policy RawReplayPolicy) *rawTrack {
	return &rawTrack{policy: policy, viewers: make(map[string]RawViewer)}
}

// recordLocked updates the replay buffer for a newly broadcast object. Caller
// holds Relay.rawMu.
func (t *rawTrack) recordLocked(obj RawObject) {
	switch t.policy {
	case ReplayNone:
		return
	case ReplayRecentRing:
		if len(t.buf) >= rawReplayRingSize {
			copy(t.buf, t.buf[1:])
			t.buf[len(t.buf)-1] = obj
		} else {
			t.buf = append(t.buf, obj)
		}
	default: // ReplayCurrentGroup
		if !t.haveGroup || obj.GroupID != t.curGroup {
			// New group: release the previous group's objects (they pin payload /
			// extension byte slices) before starting fresh.
			for i := range t.buf {
				t.buf[i] = RawObject{}
			}
			t.buf = t.buf[:0]
			t.curGroup = obj.GroupID
			t.haveGroup = true
		}
		if len(t.buf) < rawReplayMaxGroupObjects {
			t.buf = append(t.buf, obj)
		}
	}
}

// EnsureRawTrack registers a raw track (idempotent) with the replay policy that
// matches its shape, so BroadcastObject buffers correctly for late joiners. The
// policy of an already-registered track is left unchanged. Call before objects
// for the track start flowing.
func (r *Relay) EnsureRawTrack(trackKey string, policy RawReplayPolicy) {
	r.rawMu.Lock()
	if _, ok := r.rawTracks[trackKey]; !ok {
		r.rawTracks[trackKey] = newRawTrack(policy)
	}
	r.rawMu.Unlock()
}

// getOrCreateRawTrackLocked returns the track for trackKey, creating it with the
// default ReplayCurrentGroup policy if it was never registered. Creation here
// means the caller skipped pre-registration: the puller is expected to
// EnsureRawTrack with the track's correct shape before its objects flow, because
// the default is wrong for an audio track (which needs ReplayRecentRing).
// It is logged once per track (the create branch runs once) rather
// than silently defaulting, so a missed pre-registration is visible instead of
// surfacing later as a late-joiner-misses-audio symptom. Caller holds rawMu.
func (r *Relay) getOrCreateRawTrackLocked(trackKey string) *rawTrack {
	t := r.rawTracks[trackKey]
	if t == nil {
		t = newRawTrack(ReplayCurrentGroup)
		r.rawTracks[trackKey] = t
		r.log.Warn("raw track used before EnsureRawTrack; defaulting to current-group replay",
			"track", trackKey)
	}
	return t
}

// BroadcastObject fans a verbatim object out to every RawViewer subscribed to
// trackKey and updates the track's replay buffer per its policy. It buffers even
// with no viewers so a viewer joining mid-group still gets a decodable start.
func (r *Relay) BroadcastObject(trackKey string, obj RawObject) {
	r.rawMu.Lock()
	defer r.rawMu.Unlock()

	t := r.getOrCreateRawTrackLocked(trackKey)
	t.recordLocked(obj)
	for _, v := range t.viewers {
		v.SendObject(obj)
	}
}

// AddRawViewer replays the track's current buffer to the viewer, then registers
// it for live delivery. Replay and registration happen under one lock with
// BroadcastObject, so the viewer never misses an object nor receives a duplicate
// across the join.
func (r *Relay) AddRawViewer(trackKey string, v RawViewer) {
	r.rawMu.Lock()
	defer r.rawMu.Unlock()

	t := r.getOrCreateRawTrackLocked(trackKey)
	for _, o := range t.buf {
		v.SendObject(o)
	}
	t.viewers[v.ID()] = v
}

// RemoveRawViewer unregisters a raw viewer from a track by ID.
func (r *Relay) RemoveRawViewer(trackKey, id string) {
	r.rawMu.Lock()
	defer r.rawMu.Unlock()
	if t := r.rawTracks[trackKey]; t != nil {
		delete(t.viewers, id)
	}
}

// RawViewerCount returns the number of raw viewers currently subscribed to a
// track.
func (r *Relay) RawViewerCount(trackKey string) int {
	r.rawMu.RLock()
	defer r.rawMu.RUnlock()
	if t := r.rawTracks[trackKey]; t != nil {
		return len(t.viewers)
	}
	return 0
}

// defaultTrackGrace is how long a track stays subscribed upstream after its last
// downstream viewer leaves, so a viewer who flips away and back (or reconnects)
// does not force an unsubscribe/resubscribe round trip.
const defaultTrackGrace = 30 * time.Second

// trackPuller is the upstream side that Relay.AcquireTrack / ReleaseTrack drive:
// a refcount transition from 0→1 viewers Subscribes the track upstream and 1→0
// (after a grace period) Unsubscribes it. Both Subscribe and Unsubscribe are
// idempotent per track.
type trackPuller interface {
	Subscribe(trackName string)
	Unsubscribe(trackName string)
}

// upstreamPuller is the full upstream side a relayed Relay drives: the per-track
// refcount (trackPuller) plus the connection lifecycle, which is edge-triggered on
// viewer presence (EnsureUpstream / ReleaseUpstream). EnsureStarted brings the
// shared upstream connection up and Stop tears it down so a later
// EnsureStarted can restart it.
type upstreamPuller interface {
	trackPuller
	EnsureStarted(ctx context.Context)
	Stop()
}

// trackState is the per-track refcount bookkeeping for a relayed stream. Guarded
// by Relay.trackMu.
type trackState struct {
	// refs is the number of downstream viewers currently watching the track.
	refs int
	// subscribed is whether an upstream SUBSCRIBE is currently held for the
	// track. It stays true through the grace window (refs==0 but not yet
	// unsubscribed) so a re-acquire within grace needs no new SUBSCRIBE.
	subscribed bool
	// grace, when non-nil, is the pending unsubscribe timer started when refs
	// hit 0; AcquireTrack stops it on re-acquire.
	grace *time.Timer
}

// attachPuller binds the upstream puller that AcquireTrack / ReleaseTrack and the
// connection lifecycle (EnsureUpstream / ReleaseUpstream) drive, marking this relay
// as a relayed (moq→moq) stream rather than an origin one. runCtx is the parent
// context for the puller's connection goroutine: it must outlive any single viewer,
// and is cancelled when the stream is unregistered.
// Called once at registration, before the relay is exposed to any viewer.
func (r *Relay) attachPuller(p upstreamPuller, runCtx context.Context) {
	r.trackMu.Lock()
	r.puller = p
	r.upstreamCtx = runCtx
	if r.graceDur <= 0 {
		r.graceDur = defaultTrackGrace
	}
	if r.idleGraceDur <= 0 {
		r.idleGraceDur = defaultTrackGrace
	}
	r.trackMu.Unlock()
}

// isRelayed reports whether this relay re-serves an upstream stream (has a
// puller) rather than originating one locally. The MoQ session forks its
// subscribe handling on this: relayed → verbatim raw path, origin → media path.
func (r *Relay) isRelayed() bool {
	r.trackMu.Lock()
	defer r.trackMu.Unlock()
	return r.puller != nil
}

// AcquireTrack records one more downstream viewer for trackName and, on the
// 0→1 transition (or a re-acquire during the unsubscribe grace window),
// guarantees an upstream subscription. It is a no-op for an origin relay (no
// puller). The puller call happens under trackMu so a concurrent grace-expiry
// Unsubscribe and this Subscribe cannot reorder (see ReleaseTrack).
func (r *Relay) AcquireTrack(trackName string) {
	r.trackMu.Lock()
	defer r.trackMu.Unlock()
	if r.puller == nil {
		return
	}
	ts := r.tracks[trackName]
	if ts == nil {
		ts = &trackState{}
		r.tracks[trackName] = ts
	}
	if ts.grace != nil {
		ts.grace.Stop()
		ts.grace = nil
	}
	ts.refs++
	if !ts.subscribed {
		ts.subscribed = true
		r.puller.Subscribe(trackName)
	}
}

// ReleaseTrack records one fewer downstream viewer for trackName. On the 1→0
// transition it starts a grace timer; if no viewer returns before it fires the
// track is unsubscribed upstream. No-op for an origin relay or an unknown /
// already-zero track.
func (r *Relay) ReleaseTrack(trackName string) {
	r.trackMu.Lock()
	defer r.trackMu.Unlock()
	if r.puller == nil {
		return
	}
	ts := r.tracks[trackName]
	if ts == nil || ts.refs == 0 {
		return
	}
	ts.refs--
	if ts.refs == 0 && ts.grace == nil {
		ts.grace = time.AfterFunc(r.graceDur, func() { r.onGraceExpired(trackName) })
	}
}

// onGraceExpired fires when a track's unsubscribe grace window elapses. It
// unsubscribes upstream only if the track is still idle and this is still the
// active grace timer (AcquireTrack nils ts.grace when it re-acquires within the
// window, which makes this a no-op). The Unsubscribe call is under trackMu so it
// is ordered with respect to a racing AcquireTrack's Subscribe.
func (r *Relay) onGraceExpired(trackName string) {
	r.trackMu.Lock()
	defer r.trackMu.Unlock()
	ts := r.tracks[trackName]
	if ts == nil || ts.grace == nil || ts.refs != 0 {
		return
	}
	ts.grace = nil
	ts.subscribed = false
	if r.puller != nil {
		r.puller.Unsubscribe(trackName)
	}
}

// EnsureUpstream records that a viewer is connecting and guarantees the shared
// upstream connection is up. It is called by the server for every viewer of a
// relayed stream right before the catalog gate, so the first viewer's connect is
// what brings the connection up and fetches the catalog (which is what unblocks
// the gate); subsequent concurrent viewers find it already started (EnsureStarted
// is idempotent). It also cancels any pending idle-stop so a viewer arriving
// during the grace keeps the connection. No-op for an origin relay (no puller).
//
// EnsureStarted runs under trackMu (the trackMu→puller-lock order matches
// AcquireTrack), so it is fully serialised with onUpstreamIdle's Stop: a viewer
// arriving as the grace fires either bumps connRefs before the timer callback
// takes the lock (which then sees connRefs!=0 and bails) or after Stop already
// ran (and gets a fresh connection). The connection is never torn down with a
// viewer present.
func (r *Relay) EnsureUpstream() {
	r.trackMu.Lock()
	defer r.trackMu.Unlock()
	if r.puller == nil {
		return
	}
	r.connRefs++
	if r.idleStop != nil {
		r.idleStop.Stop()
		r.idleStop = nil
	}
	r.puller.EnsureStarted(r.upstreamCtx)
}

// ReleaseUpstream records that a viewer of a relayed stream has gone. On the
// transition to zero connected viewers it starts an idle-stop grace; if no viewer
// returns before it fires, the shared upstream connection is torn down (so a
// registered-but-unwatched relay holds no upstream subscription). Paired 1:1 with
// EnsureUpstream by the server (deferred), independent of AddViewer/RemoveViewer.
// No-op for an origin relay.
func (r *Relay) ReleaseUpstream() {
	r.trackMu.Lock()
	defer r.trackMu.Unlock()
	if r.puller == nil || r.connRefs == 0 {
		return
	}
	r.connRefs--
	if r.connRefs == 0 && r.idleStop == nil {
		r.idleGen++
		gen := r.idleGen
		r.idleStop = time.AfterFunc(r.idleGraceDur, func() { r.onUpstreamIdle(gen) })
	}
}

// onUpstreamIdle fires when the connection idle-stop grace elapses. It stops the
// puller only if it is still the active grace timer and no viewer has returned.
// Two independent guards make a fire-after-cancel callback safe: gen (bumped by
// every ReleaseUpstream) rejects a callback whose timer a later release/grace
// cycle superseded, and connRefs!=0 rejects one a re-acquiring EnsureUpstream
// raced past. The Stop call is under trackMu so it is ordered with respect to a
// racing EnsureUpstream's EnsureStarted.
func (r *Relay) onUpstreamIdle(gen uint64) {
	r.trackMu.Lock()
	defer r.trackMu.Unlock()
	if gen != r.idleGen {
		return
	}
	r.idleStop = nil
	if r.connRefs != 0 {
		return
	}
	if r.puller != nil {
		r.puller.Stop()
	}
}

// shutdownUpstream tears the connection lifecycle down when a relayed stream is
// unregistered. It cancels every pending timer that would otherwise keep the
// relay alive for its grace duration — the connection idle-stop and each track's
// unsubscribe grace — and stops the puller. The idleGen bump and niling each
// ts.grace make any already-fired-but-not-yet-run timer callback a no-op (they
// re-check idleGen / ts.grace under trackMu). No-op for an origin relay (no
// puller). The puller's run goroutine is also cancelled by the server via the
// upstream parent context; Stop here additionally resets the puller's state.
func (r *Relay) shutdownUpstream() {
	r.trackMu.Lock()
	defer r.trackMu.Unlock()
	if r.idleStop != nil {
		r.idleStop.Stop()
		r.idleStop = nil
	}
	r.idleGen++
	for _, ts := range r.tracks {
		if ts.grace != nil {
			ts.grace.Stop()
			ts.grace = nil
		}
	}
	if r.puller != nil {
		r.puller.Stop()
	}
}
