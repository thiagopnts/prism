package distribution

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
	"github.com/zsiec/ccx"
	"github.com/zsiec/prism/media"
	"github.com/zsiec/prism/moq"
)

// moqTrackSub holds state for a single track subscription within a MoQ session.
type moqTrackSub struct {
	requestID       uint64
	trackAlias      uint64
	trackName       string
	writer          StreamFrameWriter
	videoCh         chan *media.VideoFrame
	audioCh         chan *media.AudioFrame
	captionCh       chan *ccx.CaptionFrame
	audioTrackIndex int
	cancel          context.CancelFunc

	// rawCh is set instead of the media channels for a relayed (verbatim) track:
	// it carries upstream objects byte-for-byte to writeObjectLoop. Its presence
	// marks a raw subscription that holds an upstream refcount (see relayed.go).
	rawCh chan RawObject
}

// Compile-time interface checks.
var _ Viewer = (*MoQSession)(nil)

// StatsProviderFunc resolves the StatsProvider for a stream key lazily,
// since the pipeline may not exist when the MoQ session is created.
type StatsProviderFunc func(streamKey string) StatsProvider

// MoQSession manages a single MoQ viewer connection. It implements the Viewer
// interface so the Relay can fan out frames to it. Internally, it dispatches
// frames to per-track subscriptions, each with its own write loop and moqWriter.
type MoQSession struct {
	id                    string
	log                   *slog.Logger
	streamKey             string
	session               *webtransport.Session
	control               io.ReadWriter
	controlReader         *bufio.Reader // persistent buffered reader for control stream
	relay                 *Relay
	statsProvider         StatsProviderFunc
	controlBroadcaster    *ControlBroadcaster
	onDatagram            func(streamKey string, data []byte) []byte
	onBidirectionalStream func(streamKey string, stream io.ReadWriteCloser)
	controlMu             sync.Mutex

	mu             sync.RWMutex
	subscriptions  map[string]*moqTrackSub // key: trackName
	nextTrackAlias uint64

	damagedGroup atomic.Uint32
	closed       atomic.Bool

	videoSent      atomic.Int64
	audioSent      atomic.Int64
	captionSent    atomic.Int64
	videoDropped   atomic.Int64
	audioDropped   atomic.Int64
	captionDropped atomic.Int64
	rawDropped     atomic.Int64
	bytesSent      atomic.Int64
	lastVideoTsMS  atomic.Int64
	lastAudioTsMS  atomic.Int64
}

// MoQSessionConfig holds the parameters for creating a new MoQ session.
type MoQSessionConfig struct {
	ID                 string
	Session            *webtransport.Session
	Control            io.ReadWriter
	StreamKey          string
	Relay              *Relay
	StatsProvider      StatsProviderFunc
	ControlBroadcaster *ControlBroadcaster

	// OnDatagram is called when a WebTransport datagram arrives from a viewer.
	// The callback receives the viewer's stream key and the raw datagram bytes.
	// If the callback returns a non-nil []byte, the response is sent back to
	// the same session as a datagram (used for ping/pong clock sync).
	// Called from the session's datagram read goroutine — must not block.
	OnDatagram func(streamKey string, data []byte) []byte

	// OnBidirectionalStream is called when a viewer opens a new bidirectional
	// WebTransport stream. The callback receives the stream key and the stream.
	// It runs in its own goroutine per stream and should return when done.
	OnBidirectionalStream func(streamKey string, stream io.ReadWriteCloser)
}

// NewMoQSession creates a new MoQ session for the given stream key.
func NewMoQSession(cfg MoQSessionConfig) *MoQSession {
	return &MoQSession{
		id:                    cfg.ID,
		log:                   slog.With("session", cfg.ID, "stream", cfg.StreamKey),
		streamKey:             cfg.StreamKey,
		session:               cfg.Session,
		control:               cfg.Control,
		controlReader:         bufio.NewReader(cfg.Control),
		relay:                 cfg.Relay,
		statsProvider:         cfg.StatsProvider,
		controlBroadcaster:    cfg.ControlBroadcaster,
		onDatagram:            cfg.OnDatagram,
		onBidirectionalStream: cfg.OnBidirectionalStream,
		subscriptions:         make(map[string]*moqTrackSub),
	}
}

// ID returns the unique identifier for this MoQ session.
func (m *MoQSession) ID() string { return m.id }

// handleSetup performs the CLIENT_SETUP / SERVER_SETUP exchange.
// Returns the stream key from the PATH parameter if present.
func (m *MoQSession) handleSetup() (string, error) {
	msgType, payload, err := moq.ReadControlMsg(m.controlReader)
	if err != nil {
		return "", fmt.Errorf("read CLIENT_SETUP: %w", err)
	}
	if msgType != moq.MsgClientSetup {
		return "", fmt.Errorf("expected CLIENT_SETUP (0x%x), got 0x%x", moq.MsgClientSetup, msgType)
	}

	cs, err := moq.ParseClientSetup(payload)
	if err != nil {
		return "", fmt.Errorf("parse CLIENT_SETUP: %w", err)
	}

	// Check version compatibility
	versionOK := false
	for _, v := range cs.Versions {
		if v == moq.Version {
			versionOK = true
			break
		}
	}
	if !versionOK {
		return "", fmt.Errorf("%w (client offered %v)", moq.ErrVersionMismatch, cs.Versions)
	}

	// Send SERVER_SETUP
	ss := moq.ServerSetup{
		SelectedVersion: moq.Version,
		MaxRequestID:    100,
	}

	m.controlMu.Lock()
	err = moq.WriteControlMsg(m.control, moq.MsgServerSetup, moq.SerializeServerSetup(ss))
	m.controlMu.Unlock()
	if err != nil {
		return "", fmt.Errorf("write SERVER_SETUP: %w", err)
	}

	// Send MAX_REQUEST_ID
	m.controlMu.Lock()
	err = moq.WriteControlMsg(m.control, moq.MsgMaxRequestID, moq.SerializeMaxRequestID(100))
	m.controlMu.Unlock()
	if err != nil {
		return "", fmt.Errorf("write MAX_REQUEST_ID: %w", err)
	}

	pathKey := ""
	if cs.HasPath {
		pathKey = cs.Path
	}
	return pathKey, nil
}

// Run starts the MoQ session control loop. It blocks until the session ends.
func (m *MoQSession) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go m.readControlLoop(ctx)
	go m.readDatagramLoop(ctx)
	go m.acceptBidirectionalStreams(ctx)

	<-ctx.Done()

	m.closed.Store(true)

	// Send GOAWAY
	m.controlMu.Lock()
	_ = moq.WriteControlMsg(m.control, moq.MsgGoAway, moq.SerializeGoAway(moq.GoAway{}))
	m.controlMu.Unlock()

	// Cancel all subscriptions and release any upstream track refcounts this
	// session held. The server-level RemoveViewer does not know which upstream
	// tracks a session held, so without this a relayed stream's per-track pull
	// would never wind down when its last viewer disconnects.
	m.mu.Lock()
	subs := m.subscriptions
	m.subscriptions = make(map[string]*moqTrackSub)
	m.mu.Unlock()

	for name, sub := range subs {
		if sub.cancel != nil {
			sub.cancel()
		}
		if sub.rawCh != nil {
			m.relay.ReleaseTrack(name)
			m.relay.RemoveRawViewer(name, m.id)
		}
	}

	return ctx.Err()
}

// readControlLoop reads and dispatches control messages from the client.
func (m *MoQSession) readControlLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		msgType, payload, err := moq.ReadControlMsg(m.controlReader)
		if err != nil {
			if ctx.Err() == nil {
				m.log.Debug("control read error", "error", err)
			}
			return
		}

		switch msgType {
		case moq.MsgSubscribe:
			sub, err := moq.ParseSubscribe(payload)
			if err != nil {
				m.log.Warn("bad SUBSCRIBE", "error", err)
				continue
			}
			m.handleSubscribe(ctx, sub)

		case moq.MsgUnsubscribe:
			unsub, err := moq.ParseUnsubscribe(payload)
			if err != nil {
				m.log.Warn("bad UNSUBSCRIBE", "error", err)
				continue
			}
			m.handleUnsubscribe(unsub)

		case moq.MsgMaxRequestID:
			// Acknowledge but don't enforce client quotas
			m.log.Debug("MAX_REQUEST_ID from client")

		default:
			m.log.Debug("unknown message", "type", msgType)
		}
	}
}

// readDatagramLoop reads WebTransport datagrams and dispatches them to the
// onDatagram callback. It exits when the context is cancelled, the callback
// is nil, or the session is nil.
func (m *MoQSession) readDatagramLoop(ctx context.Context) {
	if m.onDatagram == nil || m.session == nil {
		return
	}
	for {
		data, err := m.session.ReceiveDatagram(ctx)
		if err != nil {
			if ctx.Err() == nil {
				m.log.Debug("datagram read error", "error", err)
			}
			return
		}
		if resp := m.onDatagram(m.streamKey, data); resp != nil {
			if err := m.session.SendDatagram(resp); err != nil {
				m.log.Debug("datagram response send error", "error", err)
			}
		}
	}
}

// acceptBidirectionalStreams accepts additional bidirectional streams from the
// WebTransport session (beyond the initial MoQ control stream) and dispatches
// each to the onBidirectionalStream callback in its own goroutine.
func (m *MoQSession) acceptBidirectionalStreams(ctx context.Context) {
	if m.onBidirectionalStream == nil || m.session == nil {
		return
	}
	for {
		stream, err := m.session.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() == nil {
				m.log.Debug("bidi stream accept error", "error", err)
			}
			return
		}
		go m.onBidirectionalStream(m.streamKey, stream)
	}
}

// handleSubscribe processes a SUBSCRIBE message.
func (m *MoQSession) handleSubscribe(ctx context.Context, sub moq.Subscribe) {
	// Validate namespace: must be ["prism", streamKey]
	if len(sub.Namespace) != 2 || sub.Namespace[0] != "prism" || sub.Namespace[1] != m.streamKey {
		m.sendSubscribeError(sub.RequestID, 404, moq.ErrUnknownNamespace.Error())
		return
	}

	// Only support live filter types
	if sub.FilterType != moq.FilterNextGroupStart && sub.FilterType != moq.FilterLatestObject {
		m.sendSubscribeError(sub.RequestID, 400, moq.ErrUnsupportedFilter.Error())
		return
	}

	trackName := sub.TrackName

	// Allocate track alias
	m.mu.Lock()
	alias := m.nextTrackAlias
	m.nextTrackAlias++
	m.mu.Unlock()

	// A relayed stream re-serves every non-catalog track verbatim from its
	// upstream publisher; only origin streams produce media/stats locally. The
	// catalog is served the same way for both (handleCatalogSubscribe already
	// prefers the verbatim upstream catalog), so it is left to the switch below.
	if trackName != "catalog" && m.relay.isRelayed() {
		if isRelayableTrack(trackName) {
			m.handleRawSubscribe(ctx, sub, alias, trackName)
		} else {
			m.sendSubscribeError(sub.RequestID, http.StatusNotFound, moq.ErrUnknownTrack.Error())
		}
		return
	}

	switch trackName {
	case "catalog":
		m.handleCatalogSubscribe(ctx, sub, alias)

	case "video":
		m.handleMediaSubscribe(ctx, sub, alias, trackName, "video", 0)

	case "captions":
		m.handleMediaSubscribe(ctx, sub, alias, trackName, "captions", 0)

	case "stats":
		m.handleStatsSubscribe(ctx, sub, alias)

	case "control":
		m.handleControlSubscribe(ctx, sub, alias)

	default:
		// Check for audio tracks: "audio0", "audio1", etc.
		if suffix, ok := strings.CutPrefix(trackName, "audio"); ok {
			if idx, err := strconv.Atoi(suffix); err == nil && idx >= 0 {
				m.handleMediaSubscribe(ctx, sub, alias, trackName, "audio", idx)
				return
			}
		}
		m.sendSubscribeError(sub.RequestID, 404, moq.ErrUnknownTrack.Error())
	}
}

// handleCatalogSubscribe builds and delivers the catalog, then sends SUBSCRIBE_OK.
func (m *MoQSession) handleCatalogSubscribe(ctx context.Context, sub moq.Subscribe, alias uint64) {
	// A relayed stream re-serves the upstream publisher's catalog verbatim; only
	// origin streams synthesize a catalog from observed pipeline state.
	catalogJSON := m.relay.Catalog()
	if catalogJSON == nil {
		var err error
		catalogJSON, err = buildMoQCatalog(m.streamKey, m.relay, m.controlBroadcaster != nil)
		if err != nil {
			m.sendSubscribeError(sub.RequestID, 500, "catalog build failed")
			return
		}
	}

	if err := writeCatalogObject(ctx, m.session, alias, catalogJSON); err != nil {
		m.log.Warn("catalog delivery failed", "error", err)
		m.sendSubscribeError(sub.RequestID, 500, "catalog delivery failed")
		return
	}

	m.sendSubscribeOK(sub.RequestID, alias, moq.GroupOrderAscending, true, 0, 0)
}

// handleMediaSubscribe creates a track subscription and starts the write loop.
func (m *MoQSession) handleMediaSubscribe(ctx context.Context, sub moq.Subscribe, alias uint64, trackName string, mediaType string, audioIdx int) {
	subCtx, subCancel := context.WithCancel(ctx)

	trackSub := &moqTrackSub{
		requestID:       sub.RequestID,
		trackAlias:      alias,
		trackName:       trackName,
		audioTrackIndex: audioIdx,
		cancel:          subCancel,
	}

	switch mediaType {
	case "video":
		trackSub.writer = NewMoQWriter(alias, priorityVideo)
		trackSub.videoCh = make(chan *media.VideoFrame, media.VideoBufferSize)
		// Replay the full cached GOP into the channel before starting the write
		// loop. The client-side renderer skips to the latest decoded frame, so
		// this provides immediate decodable content at the live edge.
		if n := m.relay.ReplayFullGOPToChannel(trackSub.videoCh); n > 0 {
			m.log.Debug("replayed GOP into video channel", "frames", n)
		}
		go m.writeVideoLoop(subCtx, trackSub)

	case "audio":
		trackSub.writer = NewMoQWriter(alias, priorityAudio)
		trackSub.audioCh = make(chan *media.AudioFrame, media.AudioBufferSize)
		// Replay recent audio frames into the channel before starting the write
		// loop, pre-filling the client's audio buffer for immediate playback.
		if n := m.relay.ReplayAudioToChannel(audioIdx, trackSub.audioCh); n > 0 {
			m.log.Debug("replayed audio into channel", "track", trackName, "frames", n)
		}
		go m.writeAudioLoop(subCtx, trackSub)

	case "captions":
		trackSub.writer = NewMoQWriter(alias, priorityCaptions)
		trackSub.captionCh = make(chan *ccx.CaptionFrame, viewerCaptionBuffer)
		go m.writeCaptionLoop(subCtx, trackSub)
	}

	m.mu.Lock()
	m.subscriptions[trackName] = trackSub
	m.mu.Unlock()

	m.sendSubscribeOK(sub.RequestID, alias, moq.GroupOrderAscending, false, 0, 0)

	m.log.Debug("track subscribed",
		"track", trackName,
		"alias", alias,
		"requestID", sub.RequestID)
}

// rawViewerBuffer bounds the per-track queue of verbatim objects awaiting the
// write loop. Sized to hold a replayed video GOP without dropping at the live
// edge.
const rawViewerBuffer = 256

// isRelayableTrack reports whether a relayed stream forwards trackName verbatim
// from upstream. It is the origin track set minus "control" (relayed streams
// carry no local control track) and minus "catalog" (served separately).
func isRelayableTrack(trackName string) bool {
	switch trackName {
	case "video", "captions", "stats":
		return true
	}
	if suffix, ok := strings.CutPrefix(trackName, "audio"); ok {
		if idx, err := strconv.Atoi(suffix); err == nil && idx >= 0 {
			return true
		}
	}
	return false
}

// handleRawSubscribe handles a SUBSCRIBE for a relayed stream by forwarding the
// track's upstream objects verbatim. It refcounts the upstream subscription
// (AcquireTrack, 0→1 triggers the upstream SUBSCRIBE), starts the write loop,
// then registers a raw viewer — which atomically replays the track's current
// buffer and joins live delivery, so the new viewer never misses or duplicates an
// object across the join (see Relay.AddRawViewer).
func (m *MoQSession) handleRawSubscribe(ctx context.Context, sub moq.Subscribe, alias uint64, trackName string) {
	subCtx, subCancel := context.WithCancel(ctx)

	rawCh := make(chan RawObject, rawViewerBuffer)
	trackSub := &moqTrackSub{
		requestID:  sub.RequestID,
		trackAlias: alias,
		trackName:  trackName,
		rawCh:      rawCh,
		cancel:     subCancel,
	}

	m.relay.AcquireTrack(trackName)

	// Start the write loop before registering so it drains the replay the
	// registration pushes into rawCh.
	go m.writeObjectLoop(subCtx, trackSub)

	viewer := &rawObjectViewer{id: m.id, ch: rawCh, dropped: &m.rawDropped}
	m.relay.AddRawViewer(trackName, viewer)

	m.mu.Lock()
	m.subscriptions[trackName] = trackSub
	m.mu.Unlock()

	m.sendSubscribeOK(sub.RequestID, alias, moq.GroupOrderAscending, false, 0, 0)

	m.log.Debug("raw track subscribed", "track", trackName, "alias", alias, "requestID", sub.RequestID)
}

// writeObjectLoop forwards verbatim objects for one relayed track to the viewer,
// reconstructing downstream uni-streams from the upstream stream boundaries (see
// rawStreamMux).
func (m *MoQSession) writeObjectLoop(ctx context.Context, sub *moqTrackSub) {
	mx := &rawStreamMux{
		w: NewRawStreamWriter(sub.trackAlias),
		open: func() (io.WriteCloser, error) {
			s, err := m.session.OpenUniStreamSync(ctx)
			if err != nil {
				return nil, err
			}
			return s, nil
		},
	}
	defer mx.closeCurrent()

	for {
		select {
		case <-ctx.Done():
			return
		case obj, ok := <-sub.rawCh:
			if !ok {
				return
			}
			n, err := mx.write(obj)
			if err != nil {
				m.log.Debug("raw object write failed", "track", sub.trackName, "error", err)
				return
			}
			m.bytesSent.Add(n)
		}
	}
}

// rawStreamMux reconstructs downstream uni-streams from a sequence of verbatim
// objects: it opens a new stream whenever an object starts an upstream subgroup
// stream (RawObject.StartsStream) or none is open yet, then writes the object
// with its original IDs and extension block. Mirroring the upstream's actual
// stream boundaries — rather than inferring them from GroupID, which is ambiguous
// for audio's single boundary-less stream.
type rawStreamMux struct {
	w       *RawStreamWriter
	open    func() (io.WriteCloser, error)
	current io.WriteCloser
}

func (mx *rawStreamMux) write(obj RawObject) (int64, error) {
	if obj.StartsStream || mx.current == nil {
		mx.closeCurrent()
		s, err := mx.open()
		if err != nil {
			return 0, err
		}
		if err := mx.w.WriteRawStreamHeader(s, obj.GroupID, obj.SubgroupID, obj.Priority); err != nil {
			s.Close()
			return 0, err
		}
		mx.current = s
	}
	return mx.w.WriteRawObject(mx.current, obj.ObjectID, obj.ExtBytes, obj.Payload)
}

func (mx *rawStreamMux) closeCurrent() {
	if mx.current != nil {
		mx.current.Close()
		mx.current = nil
	}
}

// rawObjectViewer adapts a track subscription's object channel to the RawViewer
// interface so a Relay can fan verbatim objects out to it. SendObject must not
// block (RawViewer contract), so it drops on a full channel.
type rawObjectViewer struct {
	id      string
	ch      chan<- RawObject
	dropped *atomic.Int64
}

// Compile-time interface check.
var _ RawViewer = (*rawObjectViewer)(nil)

func (v *rawObjectViewer) ID() string { return v.id }

func (v *rawObjectViewer) SendObject(obj RawObject) {
	select {
	case v.ch <- obj:
	default:
		v.dropped.Add(1)
	}
}

// handleUnsubscribe cancels a track subscription. For a relayed (raw) track it
// also drops the raw viewer and releases the upstream refcount so the pull can
// wind down once no viewer is watching the track.
func (m *MoQSession) handleUnsubscribe(unsub moq.Unsubscribe) {
	m.mu.Lock()
	var found *moqTrackSub
	var foundName string
	for name, sub := range m.subscriptions {
		if sub.requestID == unsub.RequestID {
			found = sub
			foundName = name
			delete(m.subscriptions, name)
			break
		}
	}
	m.mu.Unlock()

	if found == nil {
		return
	}
	if found.cancel != nil {
		found.cancel()
	}
	// Relay calls are made outside m.mu (they take the relay's locks). ReleaseTrack
	// (trackMu) precedes RemoveRawViewer (rawMu) to match the trackMu→rawMu
	// acquisition direction used elsewhere; the two are sequential, not nested.
	if found.rawCh != nil {
		m.relay.ReleaseTrack(foundName)
		m.relay.RemoveRawViewer(foundName, m.id)
	}
	m.log.Debug("track unsubscribed", "track", foundName, "requestID", unsub.RequestID)
}

// sendSubscribeOK sends a SUBSCRIBE_OK on the control stream.
func (m *MoQSession) sendSubscribeOK(requestID, trackAlias uint64, groupOrder byte, contentExists bool, largestGroup, largestObj uint64) {
	sok := moq.SubscribeOK{
		RequestID:     requestID,
		TrackAlias:    trackAlias,
		Expires:       0,
		GroupOrder:    groupOrder,
		ContentExists: contentExists,
		LargestGroup:  largestGroup,
		LargestObj:    largestObj,
	}
	m.controlMu.Lock()
	defer m.controlMu.Unlock()
	if err := moq.WriteControlMsg(m.control, moq.MsgSubscribeOK, moq.SerializeSubscribeOK(sok)); err != nil {
		m.log.Warn("write SUBSCRIBE_OK failed", "error", err)
	}
}

// sendSubscribeError sends a SUBSCRIBE_ERROR on the control stream.
func (m *MoQSession) sendSubscribeError(requestID, errorCode uint64, reason string) {
	se := moq.SubscribeError{
		RequestID:    requestID,
		ErrorCode:    errorCode,
		ReasonPhrase: reason,
	}
	m.controlMu.Lock()
	defer m.controlMu.Unlock()
	if err := moq.WriteControlMsg(m.control, moq.MsgSubscribeError, moq.SerializeSubscribeError(se)); err != nil {
		m.log.Warn("write SUBSCRIBE_ERROR failed", "error", err)
	}
}

// --- Viewer interface implementation ---

// SendVideo dispatches a video frame to the video subscription if active.
func (m *MoQSession) SendVideo(frame *media.VideoFrame) {
	m.mu.RLock()
	sub := m.subscriptions["video"]
	m.mu.RUnlock()

	if sub == nil || sub.videoCh == nil {
		return
	}

	trySendVideo(frame, sub.videoCh, &m.damagedGroup, &m.videoSent, &m.videoDropped)
}

// SendAudio dispatches an audio frame to the matching audio subscription.
func (m *MoQSession) SendAudio(frame *media.AudioFrame) {
	trackName := fmt.Sprintf("audio%d", frame.TrackIndex)

	m.mu.RLock()
	sub := m.subscriptions[trackName]
	m.mu.RUnlock()

	if sub == nil || sub.audioCh == nil {
		return
	}

	select {
	case sub.audioCh <- frame:
		m.audioSent.Add(1)
	default:
		m.audioDropped.Add(1)
	}
}

// SendCaptions dispatches a caption frame to the caption subscription.
func (m *MoQSession) SendCaptions(frame *ccx.CaptionFrame) {
	m.mu.RLock()
	sub := m.subscriptions["captions"]
	m.mu.RUnlock()

	if sub == nil || sub.captionCh == nil {
		return
	}

	select {
	case sub.captionCh <- frame:
		m.captionSent.Add(1)
	default:
		m.captionDropped.Add(1)
	}
}

// Stats returns delivery metrics for this MoQ session.
func (m *MoQSession) Stats() ViewerStats {
	return ViewerStats{
		ID:             m.id,
		VideoSent:      m.videoSent.Load(),
		AudioSent:      m.audioSent.Load(),
		CaptionSent:    m.captionSent.Load(),
		VideoDropped:   m.videoDropped.Load(),
		AudioDropped:   m.audioDropped.Load(),
		CaptionDropped: m.captionDropped.Load(),
		BytesSent:      m.bytesSent.Load(),
		LastVideoTsMS:  m.lastVideoTsMS.Load(),
		LastAudioTsMS:  m.lastAudioTsMS.Load(),
	}
}

// --- Write loops ---

func (m *MoQSession) writeVideoLoop(ctx context.Context, sub *moqTrackSub) {
	var currentStream *webtransport.SendStream
	var currentGroupID uint32

	closeStream := func() {
		if currentStream != nil {
			currentStream.Close()
			currentStream = nil
		}
	}
	defer closeStream()

	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-sub.videoCh:
			if !ok {
				return
			}

			if frame.IsKeyframe {
				closeStream()
				currentGroupID = frame.GroupID

				t0 := time.Now()
				stream, err := m.session.OpenUniStreamSync(ctx)
				if openDur := time.Since(t0); openDur > 50*time.Millisecond {
					m.log.Warn("video stream open slow",
						"duration_ms", openDur.Milliseconds(),
						"group", currentGroupID)
				}
				if err != nil {
					m.log.Debug("video stream open failed", "error", err)
					return
				}

				tsMS := uint32(frame.PTS / 1000)
				if err := sub.writer.WriteStreamHeader(stream, TrackIDVideo, currentGroupID, tsMS); err != nil {
					stream.Close()
					m.log.Debug("video header write failed", "error", err)
					return
				}
				currentStream = stream
			}

			if currentStream == nil {
				continue
			}

			t0 := time.Now()
			n, err := sub.writer.WriteVideoFrame(currentStream, frame)
			if writeDur := time.Since(t0); writeDur > 50*time.Millisecond {
				m.log.Warn("video frame write slow",
					"duration_ms", writeDur.Milliseconds(),
					"bytes", n,
					"keyframe", frame.IsKeyframe,
					"group", currentGroupID)
			}
			if err != nil {
				closeStream()
				m.log.Debug("video frame write failed", "error", err)
				return
			}
			m.bytesSent.Add(n)
			m.lastVideoTsMS.Store(frame.PTS / 1000)
		}
	}
}

func (m *MoQSession) writeAudioLoop(ctx context.Context, sub *moqTrackSub) {
	var stream *webtransport.SendStream
	defer func() {
		if stream != nil {
			stream.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-sub.audioCh:
			if !ok {
				return
			}

			if stream == nil {
				var err error
				stream, err = m.session.OpenUniStreamSync(ctx)
				if err != nil {
					m.log.Debug("audio stream open failed", "error", err)
					return
				}

				trackID := AudioTrackID(sub.audioTrackIndex)
				tsMS := uint32(frame.PTS / 1000)
				if err := sub.writer.WriteStreamHeader(stream, trackID, 0, tsMS); err != nil {
					stream.Close()
					stream = nil
					m.log.Debug("audio header write failed", "error", err)
					return
				}
			}

			tsMS := uint32(frame.PTS / 1000)
			n, err := sub.writer.WriteAudioFrame(stream, frame.Data, tsMS)
			if err != nil {
				m.log.Debug("audio frame write failed", "error", err)
				return
			}
			m.bytesSent.Add(n)
			m.lastAudioTsMS.Store(int64(tsMS))
		}
	}
}

func (m *MoQSession) writeCaptionLoop(ctx context.Context, sub *moqTrackSub) {
	var groupID uint32

	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-sub.captionCh:
			if !ok {
				return
			}

			stream, err := m.session.OpenUniStreamSync(ctx)
			if err != nil {
				m.log.Debug("caption stream open failed", "error", err)
				return
			}

			tsMS := uint32(frame.PTS / 1000)
			if err := sub.writer.WriteStreamHeader(stream, TrackIDCaptions, groupID, tsMS); err != nil {
				stream.Close()
				m.log.Debug("caption header write failed", "error", err)
				return
			}

			data := frame.Serialize()
			n, err := sub.writer.WriteDataObject(stream, data, tsMS)
			if err != nil {
				stream.Close()
				m.log.Debug("caption frame write failed", "error", err)
				return
			}

			m.bytesSent.Add(n + sub.writer.StreamHeaderSize())
			groupID++
			stream.Close()
		}
	}
}

// handleStatsSubscribe sets up the stats track subscription and starts the write loop.
func (m *MoQSession) handleStatsSubscribe(ctx context.Context, sub moq.Subscribe, alias uint64) {
	subCtx, subCancel := context.WithCancel(ctx)

	trackSub := &moqTrackSub{
		requestID:  sub.RequestID,
		trackAlias: alias,
		trackName:  "stats",
		writer:     NewMoQWriter(alias, priorityStats),
		cancel:     subCancel,
	}

	m.mu.Lock()
	m.subscriptions["stats"] = trackSub
	m.mu.Unlock()

	m.sendSubscribeOK(sub.RequestID, alias, moq.GroupOrderAscending, false, 0, 0)

	go m.writeStatsLoop(subCtx, trackSub)

	m.log.Debug("stats track subscribed",
		"alias", alias,
		"requestID", sub.RequestID)
}

// writeStatsLoop sends a StreamSnapshot as JSON every second on a new uni-stream,
// following the same pattern as writeCaptionLoop (one stream per update, incrementing groupID).
func (m *MoQSession) writeStatsLoop(ctx context.Context, sub *moqTrackSub) {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()

	var groupID uint32

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if m.closed.Load() {
				return
			}

			if m.statsProvider == nil {
				continue
			}
			provider := m.statsProvider(m.streamKey)
			if provider == nil {
				continue
			}

			viewerStats := m.Stats()
			data, err := json.Marshal(statsMessage{
				Type:        "stats",
				Stats:       provider.StreamSnapshot(),
				ViewerStats: &viewerStats,
			})
			if err != nil {
				continue
			}

			stream, err := m.session.OpenUniStreamSync(ctx)
			if err != nil {
				m.log.Debug("stats stream open failed", "error", err)
				return
			}

			tsMS := uint32(time.Now().UnixMilli())
			if err := sub.writer.WriteStreamHeader(stream, 0, groupID, tsMS); err != nil {
				stream.Close()
				m.log.Debug("stats header write failed", "error", err)
				return
			}

			n, err := sub.writer.WriteDataObject(stream, data, tsMS)
			if err != nil {
				stream.Close()
				m.log.Debug("stats write failed", "error", err)
				return
			}

			m.bytesSent.Add(n + sub.writer.StreamHeaderSize())
			groupID++
			stream.Close()
		}
	}
}

// handleControlSubscribe sets up the control track subscription and starts the write loop.
// The control track delivers application-level JSON state updates (e.g., switcher control
// room state) to connected browsers. It is only available when ControlCh is configured
// in ServerConfig. Each viewer gets its own channel from the ControlBroadcaster.
func (m *MoQSession) handleControlSubscribe(ctx context.Context, sub moq.Subscribe, alias uint64) {
	if m.controlBroadcaster == nil {
		m.sendSubscribeError(sub.RequestID, 404, "control track not available")
		return
	}

	subCtx, subCancel := context.WithCancel(ctx)

	// Subscribe to the broadcaster to get a per-session channel.
	ch := m.controlBroadcaster.Subscribe(m.id)

	trackSub := &moqTrackSub{
		requestID:  sub.RequestID,
		trackAlias: alias,
		trackName:  "control",
		writer:     NewMoQWriter(alias, priorityControl),
		cancel: func() {
			subCancel()
			m.controlBroadcaster.Unsubscribe(m.id)
		},
	}

	m.mu.Lock()
	m.subscriptions["control"] = trackSub
	m.mu.Unlock()

	m.sendSubscribeOK(sub.RequestID, alias, moq.GroupOrderAscending, false, 0, 0)

	go m.writeControlLoop(subCtx, trackSub, ch)

	m.log.Debug("control track subscribed",
		"alias", alias,
		"requestID", sub.RequestID)
}

// writeControlLoop reads JSON state snapshots from the control channel and sends
// each as a MoQ group on a new uni-stream, following the same pattern as writeStatsLoop.
func (m *MoQSession) writeControlLoop(ctx context.Context, sub *moqTrackSub, ch <-chan []byte) {
	var groupID uint32

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-ch:
			if !ok {
				return
			}

			if m.closed.Load() {
				return
			}

			stream, err := m.session.OpenUniStreamSync(ctx)
			if err != nil {
				m.log.Debug("control stream open failed", "error", err)
				return
			}

			tsMS := uint32(time.Now().UnixMilli())
			if err := sub.writer.WriteStreamHeader(stream, 0, groupID, tsMS); err != nil {
				stream.Close()
				m.log.Debug("control header write failed", "error", err)
				return
			}

			n, err := sub.writer.WriteDataObject(stream, data, tsMS)
			if err != nil {
				stream.Close()
				m.log.Debug("control write failed", "error", err)
				return
			}

			m.bytesSent.Add(n + sub.writer.StreamHeaderSize())
			groupID++
			stream.Close()
		}
	}
}
