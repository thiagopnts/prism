package distribution

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
type RawObject struct {
	GroupID    uint64
	SubgroupID uint64
	ObjectID   uint64
	Priority   byte
	ExtBytes   []byte
	Payload    []byte
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
// the default is wrong for an audio track (which needs ReplayRecentRing — see
// Gotcha #1). It is logged once per track (the create branch runs once) rather
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
