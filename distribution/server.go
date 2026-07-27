package distribution

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
	"github.com/zsiec/prism/certs"
	"github.com/zsiec/prism/moq"
)

// StatsProvider is implemented by Pipeline to supply stream statistics
// for the viewer stats overlay and the REST API.
type StatsProvider interface {
	StreamSnapshot() StreamSnapshot
}

// DebugProvider extends StatsProvider with lower-level pipeline and demuxer
// diagnostics, exposed via the /api/streams/{key}/debug endpoint.
type DebugProvider interface {
	StatsProvider
	PipelineDebug() PipelineDebugStats
	DemuxStats() *DemuxStats
}

// PipelineDebugStats captures frame forwarding counters and channel depths
// for the demux-to-relay pipeline, useful for diagnosing backpressure.
type PipelineDebugStats struct {
	VideoForwarded  int64 `json:"videoForwarded"`
	AudioForwarded  int64 `json:"audioForwarded"`
	CaptionFwd      int64 `json:"captionForwarded"`
	LastVideoFwdPTS int64 `json:"lastVideoFwdPTS"`
	LastAudioFwdPTS int64 `json:"lastAudioFwdPTS"`
	VideoChanDepth  int   `json:"videoChanDepth"`
	AudioChanDepth  int   `json:"audioChanDepth"`
}

// PipelineDebugSnapshot is the JSON response for /api/streams/{key}/debug,
// aggregating ingest, demuxer, pipeline, and viewer diagnostics.
type PipelineDebugSnapshot struct {
	Ingest   *IngestDebugStats  `json:"ingest,omitempty"`
	Demuxer  PTSDebugStats      `json:"demuxer"`
	Pipeline PipelineDebugStats `json:"pipeline"`
	Viewers  []ViewerStats      `json:"viewers"`
}

// IngestDebugStats captures SRT ingest connection metrics for the debug API.
type IngestDebugStats struct {
	BytesReceived int64  `json:"bytesReceived"`
	ReadCount     int64  `json:"readCount"`
	ConnectedAt   int64  `json:"connectedAt"`
	UptimeMs      int64  `json:"uptimeMs"`
	RemoteAddr    string `json:"remoteAddr"`
}

// StreamInfo is the JSON-serializable summary of a live stream, returned
// by the /api/streams list endpoint and used by the multi-stream viewer.
type StreamInfo struct {
	Key             string `json:"key"`
	Viewers         int    `json:"viewers"`
	Description     string `json:"description,omitempty"`
	VideoCodec      string `json:"videoCodec,omitempty"`
	Width           int    `json:"width,omitempty"`
	Height          int    `json:"height,omitempty"`
	AudioTracks     int    `json:"audioTracks,omitempty"`
	AudioChannels   int    `json:"audioChannels,omitempty"`
	HasCaptions     bool   `json:"hasCaptions,omitempty"`
	CaptionChannels []int  `json:"captionChannels,omitempty"`
	HasSCTE35       bool   `json:"hasScte35,omitempty"`
	Protocol        string `json:"protocol,omitempty"`
	UptimeMs        int64  `json:"uptimeMs,omitempty"`
}

// StreamLister is a callback that returns the current list of active streams.
type StreamLister func() []StreamInfo

// IngestLookup resolves a stream key to its ingest debug stats, or nil
// if the stream is not currently being ingested.
type IngestLookup func(key string) *IngestDebugStats

// SRTPullFunc initiates an SRT caller-mode pull from a remote address.
type SRTPullFunc func(address, streamKey, streamID string) error

// SRTStopFunc stops an active SRT pull by stream key.
type SRTStopFunc func(streamKey string) error

// SRTListFunc returns all active SRT pulls.
type SRTListFunc func() []SRTPullInfo

// SRTPullInfo describes an active SRT caller-mode pull, returned by the
// /api/srt-pull GET endpoint.
type SRTPullInfo struct {
	Address   string `json:"address"`
	StreamKey string `json:"streamKey"`
	StreamID  string `json:"streamId,omitempty"`
}

// WebTransport session close error codes sent to clients via CloseWithError.
const (
	wtErrStreamNotFound webtransport.SessionErrorCode = 1
	wtErrControlStream  webtransport.SessionErrorCode = 2
	wtErrInternal       webtransport.SessionErrorCode = 3
	wtErrBadRequest     webtransport.SessionErrorCode = 4
	wtErrSetupFailed    webtransport.SessionErrorCode = 5
)

// videoInfoTimeout is how long a new viewer waits for the first keyframe
// (and its SPS/PPS) before proceeding with default codec parameters.
const videoInfoTimeout = 30 * time.Second

// statsInterval is how often per-viewer stats snapshots are sent.
const statsInterval = 1 * time.Second

// ServerConfig holds the configuration for the distribution Server,
// including listen addresses, TLS certificate, and callback hooks.
type ServerConfig struct {
	Addr         string
	WebDir       string
	Cert         *certs.CertInfo
	TLSConfig    *tls.Config
	StreamLister StreamLister
	IngestLookup IngestLookup
	SRTPull      SRTPullFunc
	SRTStop      SRTStopFunc
	SRTList      SRTListFunc
	ExternalCert bool // true when using a CA-signed cert (not self-signed)
	ExtraRoutes  func(mux *http.ServeMux)

	// InitWindow is how long a new stream's relay observes the incoming feed to
	// learn which tracks it carries before freezing the catalog. Frames received
	// during the window are dropped (not forwarded) so playback runs at the live
	// edge with no buffering latency. If zero, defaultInitWindow (1s) is used.
	InitWindow time.Duration

	// OnStreamRegistered is called after a new stream relay is created
	// and added to the server's stream map. It is NOT called when
	// RegisterStream returns an existing relay for a duplicate key.
	// The callback is invoked outside the server's mutex.
	OnStreamRegistered func(key string, relay *Relay)

	// OnStreamUnregistered is called after a stream is removed from
	// the server's stream map. It is NOT called if the stream key
	// was not present. The callback is invoked outside the server's mutex.
	OnStreamUnregistered func(key string)

	// ControlCh receives JSON-encoded control state. If set, a "control"
	// track is advertised in the MoQ catalog and subscribers receive state
	// updates as JSON objects. Each send produces one MoQ group.
	// Messages are internally broadcast to all connected viewers via
	// ControlBroadcaster.
	ControlCh <-chan []byte

	// OnDatagram is called when a WebTransport datagram arrives from a viewer.
	// The callback receives the viewer's stream key and the raw datagram bytes.
	// If the callback returns a non-nil []byte, the response is sent back to
	// the same session as a datagram (used for ping/pong clock sync).
	// Called from the session's datagram read goroutine — must not block.
	OnDatagram func(streamKey string, data []byte) []byte

	// OnBidirectionalStream is called when a viewer opens a new bidirectional
	// WebTransport stream (beyond the initial MoQ control stream). The callback
	// receives the viewer's stream key and the stream itself. The callback is
	// responsible for reading from and writing to the stream; it runs in its
	// own goroutine and should return when done. The stream is automatically
	// accepted from the session's accept loop.
	OnBidirectionalStream func(streamKey string, stream io.ReadWriteCloser)

	// OnViewerAdded is called after a MoQ viewer is added to a relay.
	// The callback receives the stream key the viewer subscribed to.
	// Called from the handleMoQ goroutine — must not block.
	OnViewerAdded func(streamKey string)

	// QUICConfig overrides the default quic-go configuration for the
	// underlying HTTP/3 + WebTransport server. When nil, sensible
	// defaults are used (30s idle timeout, 0-RTT enabled, default
	// flow control windows). Callers that stream live media should
	// set larger flow control windows here — the quic-go defaults
	// (512 KB stream / 768 KB connection) are too small for sustained
	// video bitrates and cause server-side write blocking.
	QUICConfig *quic.Config

	// ExternalUpstreamResolver resolves an `external=` query key (an extra feed
	// to merge into the stream a viewer requests) to the prism host it must be
	// pulled from over MoQ. Returns ok=false for an unknown key, which fails that
	// viewer's connection cleanly. This is the MoQ merge path; it is consulted only
	// when VirtualExternalResolver is nil or declines the key. When both are nil,
	// any `external=` request is rejected.
	ExternalUpstreamResolver func(externalKey string) (UpstreamConfig, bool)

	// PrimaryUpstreamResolver resolves the primary stream key a viewer requests
	// (via ?stream= or the PATH parameter) to the prism host it must be pulled
	// from over MoQ, on a GetRelay miss. Returns ok=false for an unknown/gone
	// feed, which closes the viewer's connection cleanly with stream-not-found
	// (the pre-resolver behavior on a miss). Unlike ExternalUpstreamResolver (the
	// external= merge path), this drives the primary stream a viewer watches and
	// is re-invoked by the relay puller after a connection failure to follow host
	// failover (SetUpstream + retry) or evict a feed that has gone away. It may do
	// I/O (e.g. a registry lookup); it is never called while Server.mu is held.
	PrimaryUpstreamResolver func(ctx context.Context, streamKey string) (UpstreamConfig, bool)

	// VirtualExternalResolver resolves an `external=` query key to a
	// caller-assembled virtual track (e.g. captions built from an HTTP feed)
	// instead of a MoQ upstream. When set, it is consulted first for every
	// `external=` key; a returned source is merged into the anchor's catalog and
	// its frames are paced against the anchor timeline (see VirtualExternalSource).
	// Returns ok=false to fall back to ExternalUpstreamResolver (the MoQ path).
	VirtualExternalResolver func(externalKey string) (VirtualExternalSource, bool)
}

// streamResources bundles the relay and stats provider for a single live
// stream, ensuring both are registered and torn down as a unit. For a relayed
// (moq→moq) stream registered via RegisterRelayedUpstream it also holds the
// puller and the cancel for its connection parent context, both torn down with
// the stream.
type streamResources struct {
	relay          *Relay
	pipeline       StatsProvider
	puller         *RelayPuller       // non-nil only for RegisterRelayedUpstream streams
	upstreamCancel context.CancelFunc // cancels the puller's connection parent context
}

// Server is the WebTransport/HTTP3 distribution server. It manages relays,
// pipelines, viewer sessions, and serves both the WebTransport watch
// endpoints and the REST API.
type Server struct {
	config ServerConfig
	wtSrv  *webtransport.Server

	mu      sync.RWMutex
	streams map[string]*streamResources
	// externalRelays holds a shared relayed puller per `external=` feed key, kept
	// separate from streams so an external key can never collide with a real
	// stream key. Built lazily by resolveExternalRelayLocked; reused across every
	// merge relay that references the key.
	externalRelays map[string]*streamResources
	// mergeRelays holds one synthetic merge relay per (anchor stream + sorted
	// external set), keyed by an internal cache key that never appears on the wire.
	// Reused when the same combination is requested again; swept when the anchor
	// stream is unregistered.
	mergeRelays map[string]*mergeResources

	controlBroadcaster *ControlBroadcaster // nil if ControlCh not configured
}

// mergeResources bundles a synthetic merge relay with its MergePuller and the
// cancel for its upstream parent context, plus the anchor stream key so the
// unregister sweep can tear it down when the anchor goes away.
type mergeResources struct {
	relay          *Relay
	puller         *MergePuller
	anchorKey      string
	upstreamCancel context.CancelFunc
}

// NewServer creates a distribution Server with the given configuration.
// It returns an error if required fields are missing.
func NewServer(config ServerConfig) (*Server, error) {
	if config.Cert == nil && config.TLSConfig == nil {
		return nil, errors.New("distribution: either Cert or TLSConfig is required")
	}
	if config.Addr == "" {
		return nil, errors.New("distribution: Addr is required")
	}
	s := &Server{
		config:         config,
		streams:        make(map[string]*streamResources),
		externalRelays: make(map[string]*streamResources),
		mergeRelays:    make(map[string]*mergeResources),
	}
	if config.ControlCh != nil {
		s.controlBroadcaster = NewControlBroadcaster()
	}
	return s, nil
}

// RegisterStream creates a Relay for the given stream key and returns it.
// If the stream already has a relay, the existing one is returned.
// For new streams, OnStreamRegistered is called (if set) after releasing
// the lock. Concurrent calls with the same key are safe (only one creates
// a relay), but the callback may observe transient inconsistency if a
// concurrent UnregisterStream for the same key interleaves between the
// lock release and the callback invocation.
func (s *Server) RegisterStream(streamKey string) *Relay {
	s.mu.Lock()
	if sr, ok := s.streams[streamKey]; ok {
		s.mu.Unlock()
		return sr.relay
	}
	r := NewRelay()
	window := s.config.InitWindow
	if window <= 0 {
		window = defaultInitWindow
	}
	r.SetInitWindow(window)
	s.streams[streamKey] = &streamResources{relay: r}
	s.mu.Unlock()

	if s.config.OnStreamRegistered != nil {
		s.config.OnStreamRegistered(streamKey, r)
	}
	return r
}

// RegisterRelayedStream creates a Relay for streamKey that re-serves the given
// verbatim upstream catalog to viewers, instead of synthesizing one from
// observed pipeline state. Use it for relay/pull tiers that forward another
// publisher's already-final stream: no init window is applied and viewers are
// not gated on the first keyframe (see Relay.SetCatalog). If the stream already
// has a relay, its catalog is updated to the supplied bytes and the existing
// relay is returned. For new streams, OnStreamRegistered is called (if set)
// after releasing the lock.
func (s *Server) RegisterRelayedStream(streamKey string, catalog []byte) *Relay {
	s.mu.Lock()
	if sr, ok := s.streams[streamKey]; ok {
		s.mu.Unlock()
		sr.relay.SetCatalog(catalog)
		return sr.relay
	}
	r := NewRelay()
	r.SetCatalog(catalog)
	s.streams[streamKey] = &streamResources{relay: r}
	s.mu.Unlock()

	if s.config.OnStreamRegistered != nil {
		s.config.OnStreamRegistered(streamKey, r)
	}
	return r
}

// RegisterRelayedUpstream registers a relayed (moq→moq) stream that lazily pulls
// from an upstream prism distribution server. It is pure bookkeeping: it creates a
// cold relay, binds a RelayPuller for cfg, and records both — but opens no
// connection and fetches no catalog. The pull starts only when the first viewer
// connects (handleMoQ calls Relay.EnsureUpstream before the catalog gate), so a
// registered-but-unwatched stream costs nothing. If cfg.StreamKey is empty it
// defaults to streamKey (the common case where the local and upstream keys match).
//
// If the stream is already registered, its upstream is updated in place (addr /
// cert-hash change applied via the puller, see SetRelayedUpstream) and the
// existing relay is returned. For new streams, OnStreamRegistered is called (if
// set) after releasing the lock.
func (s *Server) RegisterRelayedUpstream(streamKey string, cfg UpstreamConfig) *Relay {
	if cfg.StreamKey == "" {
		cfg.StreamKey = streamKey
	}

	s.mu.Lock()
	if sr, ok := s.streams[streamKey]; ok {
		// Apply any upstream change while still holding s.mu, so a concurrent
		// UnregisterStream cannot delete + cancel this stream between the lookup and
		// the update and leave us returning an orphaned relay whose upstream context
		// is already cancelled. SetUpstream only takes the puller lock — s.mu →
		// puller.mu is a fresh, acyclic order (nothing takes s.mu under puller.mu).
		if sr.puller != nil {
			sr.puller.SetUpstream(cfg.Addr, cfg.CertHashes)
		}
		relay := sr.relay
		s.mu.Unlock()
		return relay
	}
	r := NewRelay()
	upCtx, upCancel := context.WithCancel(context.Background())
	puller := NewRelayPuller(r, cfg)
	r.attachPuller(puller, upCtx)
	s.streams[streamKey] = &streamResources{relay: r, puller: puller, upstreamCancel: upCancel}
	s.mu.Unlock()

	if s.config.OnStreamRegistered != nil {
		s.config.OnStreamRegistered(streamKey, r)
	}
	return r
}

// SetRelayedUpstream updates the upstream dial target and cert-hash trust set for
// an already-registered relayed stream (e.g. cert rotation or address failover),
// without tearing down a healthy connection when nothing changed. It returns true
// if the upstream actually changed (a live connection is dropped so the puller
// reconnects against it), false on a no-op or an unknown / non-relayed stream.
func (s *Server) SetRelayedUpstream(streamKey, addr string, certHashes []string) bool {
	s.mu.RLock()
	sr := s.streams[streamKey]
	s.mu.RUnlock()
	if sr == nil || sr.puller == nil {
		return false
	}
	return sr.puller.SetUpstream(addr, certHashes)
}

// resolveExternalRelayLocked returns the shared relayed relay for an `external=`
// feed key, building it lazily via ExternalUpstreamResolver on first use. Like
// RegisterRelayedUpstream it is pure bookkeeping — no connection is opened until a
// MergePuller calls EnsureUpstream on the returned relay. Caller holds s.mu.
func (s *Server) resolveExternalRelayLocked(externalKey string) (*Relay, error) {
	if sr, ok := s.externalRelays[externalKey]; ok {
		return sr.relay, nil
	}
	if s.config.ExternalUpstreamResolver == nil {
		return nil, fmt.Errorf("external feed resolver not configured")
	}
	cfg, ok := s.config.ExternalUpstreamResolver(externalKey)
	if !ok {
		return nil, fmt.Errorf("unknown external feed %q", externalKey)
	}
	if cfg.StreamKey == "" {
		cfg.StreamKey = externalKey
	}
	r := NewRelay()
	upCtx, upCancel := context.WithCancel(context.Background())
	puller := NewRelayPuller(r, cfg)
	r.attachPuller(puller, upCtx)
	s.externalRelays[externalKey] = &streamResources{relay: r, puller: puller, upstreamCancel: upCancel}
	return r, nil
}

// resolvePrimaryRelay builds (or returns the already-registered) relay for a
// primary stream key on a GetRelay miss, resolving the upstream via
// PrimaryUpstreamResolver. Like RegisterRelayedUpstream it is pure bookkeeping:
// it creates a cold relay + RelayPuller and records both, opening no connection
// until the first viewer's EnsureUpstream. It returns ok=false when no resolver
// is configured or the resolver declines (unknown/gone feed), so the caller
// closes the viewer with stream-not-found.
//
// The resolver call may do I/O, so — unlike resolveExternalRelayLocked — it runs
// WITHOUT Server.mu held. A double-check under the lock makes a concurrent
// resolve of the same key return the single winner (the loser discards its
// unused cfg; nothing was dialed).
func (s *Server) resolvePrimaryRelay(ctx context.Context, streamKey string) (*Relay, bool) {
	if s.config.PrimaryUpstreamResolver == nil {
		return nil, false
	}

	s.mu.RLock()
	if sr, ok := s.streams[streamKey]; ok {
		r := sr.relay
		s.mu.RUnlock()
		return r, true
	}
	s.mu.RUnlock()

	cfg, ok := s.config.PrimaryUpstreamResolver(ctx, streamKey)
	if !ok {
		return nil, false
	}
	if cfg.StreamKey == "" {
		cfg.StreamKey = streamKey
	}

	s.mu.Lock()
	if sr, ok := s.streams[streamKey]; ok { // lost the resolve race; use the winner
		r := sr.relay
		s.mu.Unlock()
		return r, true
	}
	r := NewRelay()
	upCtx, upCancel := context.WithCancel(context.Background())
	puller := NewRelayPuller(r, cfg)
	// Re-resolve on connection failure to follow SRT-host failover / cert
	// rotation, and evict the stream when the feed is gone. Same-package field
	// assignment — see RelayPuller.run / applyReResolve.
	puller.reResolve = func(ctx context.Context) (UpstreamConfig, bool) {
		return s.config.PrimaryUpstreamResolver(ctx, streamKey)
	}
	puller.onGone = func() { s.UnregisterStream(streamKey) }
	r.attachPuller(puller, upCtx)
	s.streams[streamKey] = &streamResources{relay: r, puller: puller, upstreamCancel: upCancel}
	s.mu.Unlock()

	if s.config.OnStreamRegistered != nil {
		s.config.OnStreamRegistered(streamKey, r)
	}
	return r, true
}

// resolveMergeRelay returns the synthetic merge relay for the given anchor stream
// plus external feed set, building it on first request and reusing it for any
// later request with the same (anchor, sorted-externals) combination. The returned
// relay reports the plain anchor stream key on the wire (its merged catalog's
// namespace is the anchor key), so an unmodified consumer subscribes every merged
// track under the one namespace it knows; the cache key is internal only.
//
// The whole call runs under s.mu: construction is synchronous bookkeeping (no I/O
// — the taps dial lazily when the first viewer's EnsureUpstream starts the
// MergePuller), so holding the lock cannot block on the network.
//
// The anchor relay is re-fetched from s.streams here rather than trusting a
// pointer resolved earlier: setupMoQ resolves the anchor before a network
// handshake, and the anchor can be unregistered (and its merge relays swept) in
// that window. Building the entry only while the anchor is present in s.streams
// guarantees a later unregister sweep covers it (closing the sweep-then-insert
// TOCTOU) and binds the merge to the currently-live anchor relay.
func (s *Server) resolveMergeRelay(anchorKey string, externalKeys []string) (*Relay, error) {
	sorted := append([]string(nil), externalKeys...)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	cacheKey := anchorKey + "\x00" + strings.Join(sorted, "\x00")

	s.mu.Lock()
	defer s.mu.Unlock()

	if mr, ok := s.mergeRelays[cacheKey]; ok {
		return mr.relay, nil
	}

	sr, ok := s.streams[anchorKey]
	if !ok {
		return nil, fmt.Errorf("anchor stream %q not found", anchorKey)
	}
	anchor := sr.relay

	externals := make([]mergeExternal, 0, len(sorted))
	for _, k := range sorted {
		// Prefer the virtual (caller-assembled) path: a resolved source is merged
		// with no upstream pull. Fall back to the MoQ path only when no virtual
		// resolver is configured or it declines the key.
		if s.config.VirtualExternalResolver != nil {
			if src, ok := s.config.VirtualExternalResolver(k); ok {
				externals = append(externals, mergeExternal{key: k, virtual: src})
				continue
			}
		}
		er, err := s.resolveExternalRelayLocked(k)
		if err != nil {
			return nil, err
		}
		externals = append(externals, mergeExternal{key: k, relay: er})
	}

	mergeRelay := NewRelay()
	upCtx, upCancel := context.WithCancel(context.Background())
	mp := NewMergePuller(mergeRelay, cacheKey, anchorKey, anchor, externals)
	mergeRelay.attachPuller(mp, upCtx)
	s.mergeRelays[cacheKey] = &mergeResources{
		relay:          mergeRelay,
		puller:         mp,
		anchorKey:      anchorKey,
		upstreamCancel: upCancel,
	}
	return mergeRelay, nil
}

// RefreshExternalUpstreams re-resolves every shared external relay's upstream via
// ExternalUpstreamResolver and applies any change to its puller. External feeds
// are pulled from a cluster whose addr / QUIC cert hashes are discovered out of
// band (e.g. polling that cluster's cert-hash endpoint) and can rotate; call this
// after such a poll observes a change so live external pulls pick up the new addr
// or cert hashes instead of failing TLS on their next reconnect. Each SetUpstream
// is a no-op when nothing changed, so calling this on a fixed schedule is cheap.
func (s *Server) RefreshExternalUpstreams() {
	if s.config.ExternalUpstreamResolver == nil {
		return
	}
	type ext struct {
		key    string
		puller *RelayPuller
	}
	s.mu.RLock()
	exts := make([]ext, 0, len(s.externalRelays))
	for k, sr := range s.externalRelays {
		if sr.puller != nil {
			exts = append(exts, ext{key: k, puller: sr.puller})
		}
	}
	s.mu.RUnlock()

	// Resolve + SetUpstream outside s.mu: the resolver is a cheap cached read and
	// SetUpstream takes only the puller's own lock (s.mu → puller.mu is never
	// nested, matching SetRelayedUpstream).
	for _, e := range exts {
		if cfg, ok := s.config.ExternalUpstreamResolver(e.key); ok {
			e.puller.SetUpstream(cfg.Addr, cfg.CertHashes)
		}
	}
}

// UnregisterStream removes the relay and pipeline for a stream key.
// If the stream existed, OnStreamUnregistered is called (if set) after
// releasing the lock. If a concurrent RegisterStream for the same key
// races with this call, the callback may fire after a new relay has
// already been registered.
func (s *Server) UnregisterStream(streamKey string) {
	s.unregister(streamKey)
}

// UnregisterRelayedStream removes a relayed stream registered via
// RegisterRelayedUpstream, stopping its puller and cancelling its connection
// parent context. It is the named counterpart to RegisterRelayedUpstream; the
// teardown is identical to UnregisterStream (which also stops a puller if present),
// so either is safe to call.
func (s *Server) UnregisterRelayedStream(streamKey string) {
	s.unregister(streamKey)
}

// unregister removes a stream and tears down any puller it held, then fires
// OnStreamUnregistered (only if the stream existed). Cancelling upstreamCancel
// stops the connection goroutine; relay.shutdownUpstream stops the puller and
// cancels every pending grace/idle timer so none keeps the unregistered relay
// alive for its grace duration. Both are no-ops for an origin stream.
func (s *Server) unregister(streamKey string) {
	s.mu.Lock()
	sr, existed := s.streams[streamKey]
	delete(s.streams, streamKey)
	// Sweep any merge relays anchored on this stream: they synthesize their
	// catalog from the anchor and are meaningless once it is gone.
	var mergeEvicted []*mergeResources
	for ck, mr := range s.mergeRelays {
		if mr.anchorKey == streamKey {
			mergeEvicted = append(mergeEvicted, mr)
			delete(s.mergeRelays, ck)
		}
	}
	s.mu.Unlock()

	for _, mr := range mergeEvicted {
		if mr.upstreamCancel != nil {
			mr.upstreamCancel()
		}
		mr.relay.shutdownUpstream()
	}

	if !existed {
		return
	}
	if sr.upstreamCancel != nil {
		sr.upstreamCancel()
	}
	sr.relay.shutdownUpstream()
	if s.config.OnStreamUnregistered != nil {
		s.config.OnStreamUnregistered(streamKey)
	}
}

// SetPipeline associates a StatsProvider with a stream key. The stream
// must already be registered via RegisterStream.
func (s *Server) SetPipeline(streamKey string, p StatsProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sr, ok := s.streams[streamKey]; ok {
		sr.pipeline = p
	}
}

// GetPipeline returns the StatsProvider for a stream key, or nil if not found.
func (s *Server) GetPipeline(streamKey string) StatsProvider {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if sr, ok := s.streams[streamKey]; ok {
		return sr.pipeline
	}
	return nil
}

// GetRelay returns the Relay for a stream key, or nil if not found.
func (s *Server) GetRelay(streamKey string) *Relay {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if sr, ok := s.streams[streamKey]; ok {
		return sr.relay
	}
	return nil
}

// registerAPIRoutes registers the REST API endpoints on the given mux.
func (s *Server) registerAPIRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/streams", s.handleListStreams)
	mux.HandleFunc("GET /api/streams/{key}/debug", s.handleStreamDebug)
	mux.HandleFunc("GET /api/cert-hash", s.handleCertHash)
	mux.HandleFunc("GET /api/srt-pull", s.handleSRTPullList)
	mux.HandleFunc("POST /api/srt-pull", s.handleSRTPullCreate)
	mux.HandleFunc("DELETE /api/srt-pull", s.handleSRTPullStop)
	mux.HandleFunc("OPTIONS /api/srt-pull", s.handleSRTPullOptions)

	if s.config.ExtraRoutes != nil {
		s.config.ExtraRoutes(mux)
	}
}

// APIHandler returns an http.Handler for the HTTPS REST API, including
// stream listing, debug endpoints, cert hash, and SRT pull management.
func (s *Server) APIHandler() http.Handler {
	mux := http.NewServeMux()
	s.registerAPIRoutes(mux)

	if s.config.WebDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(s.config.WebDir)))
	}

	return corsMiddleware(crossOriginIsolationMiddleware(mux))
}

func crossOriginIsolationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
		w.Header().Set("Cross-Origin-Embedder-Policy", "require-corp")
		next.ServeHTTP(w, r)
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encoding JSON response", "error", err)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// Start launches the HTTP/3 WebTransport server and blocks until the context
// is cancelled or a fatal error occurs.
func (s *Server) Start(ctx context.Context) error {
	wtMux := http.NewServeMux()
	wtMux.HandleFunc("/moq", s.handleMoQ)
	s.registerAPIRoutes(wtMux)

	var baseTLS *tls.Config
	if s.config.TLSConfig != nil {
		baseTLS = s.config.TLSConfig
	} else {
		baseTLS = &tls.Config{
			Certificates: []tls.Certificate{s.config.Cert.TLSCert},
		}
	}
	tlsConfig := http3.ConfigureTLSConfig(baseTLS)

	qc := s.config.QUICConfig
	if qc == nil {
		qc = &quic.Config{
			MaxIdleTimeout: 30 * time.Second,
			Allow0RTT:      true,
		}
	}

	h3srv := &http3.Server{
		Addr:       s.config.Addr,
		Handler:    corsMiddleware(wtMux),
		TLSConfig:  tlsConfig,
		QUICConfig: qc,
	}
	webtransport.ConfigureHTTP3Server(h3srv)

	s.wtSrv = &webtransport.Server{
		H3: h3srv,
		// SECURITY: CheckOrigin accepts all origins. This is intentional for
		// development and local-network use. Production deployments behind a
		// reverse proxy should enforce origin checks at the proxy layer.
		CheckOrigin: func(_ *http.Request) bool {
			return true
		},
	}

	if s.controlBroadcaster != nil {
		go s.controlBroadcaster.Run(ctx, s.config.ControlCh)
	}

	slog.Info("WebTransport server listening", "addr", s.config.Addr)

	stop := context.AfterFunc(ctx, func() { s.wtSrv.Close() })
	defer stop()

	err := s.wtSrv.ListenAndServe()
	if ctx.Err() != nil {
		return nil
	}
	return err
}

type statsMessage struct {
	Type        string         `json:"type"`
	Stats       StreamSnapshot `json:"stats"`
	ViewerStats *ViewerStats   `json:"viewerStats,omitempty"`
}

type certHashResponse struct {
	Hash    string `json:"hash"`
	Addr    string `json:"addr"`
	Trusted bool   `json:"trusted"`
}

func (s *Server) handleMoQ(w http.ResponseWriter, r *http.Request) {
	session, controlStream, err := s.upgradeMoQ(w, r)
	if err != nil {
		return // upgradeMoQ already logged and closed the session
	}

	streamKey, relay, moqSession, err := s.setupMoQ(r, session, controlStream)
	if err != nil {
		return // setupMoQ already logged and closed the session
	}

	// For a relayed (moq→moq) stream the upstream is pulled lazily: this viewer's
	// connect brings the shared connection up and fetches the catalog, which is
	// what unblocks the WaitCatalogReady gate below; if the upstream is down the
	// gate times out cleanly for just this viewer. ReleaseUpstream (deferred) drops
	// the connection-presence refcount so the connection is torn down after a grace
	// once the last viewer leaves. Both are no-ops for an origin stream (no puller).
	relay.EnsureUpstream()
	defer relay.ReleaseUpstream()

	waitCtx, waitCancel := context.WithTimeout(r.Context(), videoInfoTimeout)
	defer waitCancel()
	relay.WaitVideoInfo(waitCtx)
	// Wait for the init window to close so the catalog reflects the feed's full
	// track set before this viewer can subscribe to it.
	relay.WaitCatalogReady(waitCtx)

	relay.AddViewer(moqSession)
	defer relay.RemoveViewer(moqSession.ID())

	if s.config.OnViewerAdded != nil {
		s.config.OnViewerAdded(streamKey)
	}

	_ = streamKey // used during setup; kept for clarity
	if err := moqSession.Run(session.Context()); err != nil {
		slog.Debug("moq session ended", "session", moqSession.ID(), "error", err)
	}
}

// upgradeMoQ upgrades the HTTP request to a WebTransport session and accepts
// the bidirectional control stream. On failure it logs, closes the session,
// and returns a non-nil error.
func (s *Server) upgradeMoQ(w http.ResponseWriter, r *http.Request) (*webtransport.Session, *webtransport.Stream, error) {
	session, err := s.wtSrv.Upgrade(w, r)
	if err != nil {
		slog.Error("webtransport upgrade failed (moq)", "error", err)
		return nil, nil, err
	}

	slog.Info("moq viewer connected", "remote", r.RemoteAddr)

	controlStream, err := session.AcceptStream(r.Context())
	if err != nil {
		slog.Error("failed to accept moq control stream", "error", err)
		session.CloseWithError(wtErrControlStream, "control stream error")
		return nil, nil, err
	}

	return session, controlStream, nil
}

// setupMoQ performs the MoQ handshake, resolves the stream key (from URL query
// or PATH parameter), and returns the relay and session. On failure it logs,
// closes the session, and returns a non-nil error.
func (s *Server) setupMoQ(r *http.Request, session *webtransport.Session, controlStream *webtransport.Stream) (string, *Relay, *MoQSession, error) {
	streamKey := r.URL.Query().Get("stream")

	relay := s.GetRelay(streamKey)
	if relay == nil && streamKey != "" {
		if resolved, ok := s.resolvePrimaryRelay(r.Context(), streamKey); ok {
			relay = resolved
		} else {
			slog.Warn("moq stream not found", "stream", streamKey)
			session.CloseWithError(wtErrStreamNotFound, "stream not found")
			return "", nil, nil, moq.ErrUnknownTrack
		}
	}

	moqSession := NewMoQSession(MoQSessionConfig{
		ID:                    fmt.Sprintf("moq-%s-%s", streamKey, r.RemoteAddr),
		Session:               session,
		Control:               controlStream,
		StreamKey:             streamKey,
		Relay:                 relay,
		StatsProvider:         s.GetPipeline,
		ControlBroadcaster:    s.controlBroadcaster,
		OnDatagram:            s.config.OnDatagram,
		OnBidirectionalStream: s.config.OnBidirectionalStream,
	})

	pathKey, err := moqSession.handleSetup()
	if err != nil {
		slog.Warn("moq setup failed", "error", err)
		session.CloseWithError(wtErrSetupFailed, "setup failed")
		return "", nil, nil, err
	}

	// PATH parameter may override URL query stream key
	if pathKey != "" && streamKey == "" {
		streamKey = pathKey
		moqSession.streamKey = streamKey
		relay = s.GetRelay(streamKey)
		if relay == nil {
			if resolved, ok := s.resolvePrimaryRelay(r.Context(), streamKey); ok {
				relay = resolved
			} else {
				slog.Warn("moq stream not found (from PATH)", "stream", streamKey)
				session.CloseWithError(wtErrStreamNotFound, "stream not found")
				return "", nil, nil, moq.ErrUnknownTrack
			}
		}
		moqSession.relay = relay
	}

	if streamKey == "" {
		slog.Warn("moq no stream key provided")
		session.CloseWithError(wtErrBadRequest, "missing stream key")
		return "", nil, nil, fmt.Errorf("moq: missing stream key")
	}

	// Extra `external=` feeds merge into the stream this viewer requested: swap
	// the anchor relay for a shared merge relay that re-serves the anchor's tracks
	// plus one per external, all under the anchor's (unchanged) stream key. With no
	// externals the path is untouched — the viewer gets the anchor relay directly.
	if externals := r.URL.Query()["external"]; len(externals) > 0 {
		merged, err := s.resolveMergeRelay(streamKey, externals)
		if err != nil {
			slog.Warn("moq merge relay setup failed", "stream", streamKey, "error", err)
			session.CloseWithError(wtErrStreamNotFound, "merge stream unavailable")
			return "", nil, nil, err
		}
		relay = merged
		moqSession.relay = merged
	}

	return streamKey, relay, moqSession, nil
}

func (s *Server) handleListStreams(w http.ResponseWriter, _ *http.Request) {
	var resp []StreamInfo

	if s.config.StreamLister != nil {
		resp = s.config.StreamLister()
	}

	resp = s.appendRelayedStreamInfos(resp)

	if resp == nil {
		resp = make([]StreamInfo, 0)
	}

	writeJSON(w, http.StatusOK, resp)
}

// RelayedStreamInfos returns a StreamInfo for every relayed (moq→moq) stream
// registered on this server: idle streams show minimal data (key, protocol,
// viewer count); actively-relayed ones add codec / resolution / track counts
// parsed read-only from the verbatim catalog they already hold. It is the
// public, origin-free counterpart of the relayed half of handleListStreams, for
// callers that serve their own /api/streams on a separate listener from this
// server's WebTransport endpoint and want to merge relayed streams into it.
// Nothing is fetched eagerly.
func (s *Server) RelayedStreamInfos() []StreamInfo {
	infos := s.appendRelayedStreamInfos(nil)
	if infos == nil {
		return []StreamInfo{}
	}
	return infos
}

// appendRelayedStreamInfos adds a StreamInfo for every relayed (moq→moq) stream
// not already present in existing (deduped by key, so a StreamLister entry wins).
// An idle relayed stream shows minimal data (key, protocol, viewer count); an
// actively-relayed one additionally has codec / resolution / track counts parsed
// read-only from the verbatim upstream catalog it already holds.
func (s *Server) appendRelayedStreamInfos(existing []StreamInfo) []StreamInfo {
	seen := make(map[string]struct{}, len(existing))
	for _, si := range existing {
		seen[si.Key] = struct{}{}
	}

	type relayedStream struct {
		key   string
		relay *Relay
	}
	s.mu.RLock()
	relayed := make([]relayedStream, 0, len(s.streams))
	for key, sr := range s.streams {
		if sr.puller != nil {
			relayed = append(relayed, relayedStream{key: key, relay: sr.relay})
		}
	}
	s.mu.RUnlock()

	for _, e := range relayed {
		if _, dup := seen[e.key]; dup {
			slog.Debug("Ignoring duplicated stream", "feed_id", e.key)
			continue
		}
		si := StreamInfo{
			Key:      e.key,
			Protocol: "moq-relay",
			Viewers:  e.relay.ViewerCount(),
		}
		if cat := e.relay.Catalog(); cat != nil {
			applyCatalogInfo(&si, cat)
		}
		existing = append(existing, si)
	}
	return existing
}

// applyCatalogInfo fills the display-only codec / resolution / track-count fields
// of si from a verbatim MoQ catalog by unmarshalling it read-only into the same
// struct buildMoQCatalog produces. A malformed catalog leaves si unchanged.
func applyCatalogInfo(si *StreamInfo, catalog []byte) {
	var c moqCatalog
	if err := json.Unmarshal(catalog, &c); err != nil {
		return
	}
	for _, t := range c.Tracks {
		switch {
		case t.Name == "video":
			si.VideoCodec = t.SelectionParams.Codec
			si.Width = t.SelectionParams.Width
			si.Height = t.SelectionParams.Height
		case t.Name == "captions":
			si.HasCaptions = true
		case strings.HasPrefix(t.Name, "audio"):
			si.AudioTracks++
		}
	}
}

func (s *Server) handleStreamDebug(w http.ResponseWriter, r *http.Request) {
	streamKey := r.PathValue("key")

	s.mu.RLock()
	sr := s.streams[streamKey]
	s.mu.RUnlock()

	if sr == nil || sr.pipeline == nil {
		writeError(w, http.StatusNotFound, "stream not found")
		return
	}

	var snap PipelineDebugSnapshot

	if dp, ok := sr.pipeline.(DebugProvider); ok {
		snap.Pipeline = dp.PipelineDebug()
		snap.Demuxer = dp.DemuxStats().PTSDebug()
	}

	snap.Viewers = sr.relay.ViewerStatsAll()

	if s.config.IngestLookup != nil {
		snap.Ingest = s.config.IngestLookup(streamKey)
	}

	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) handleCertHash(w http.ResponseWriter, _ *http.Request) {
	var hash string
	if s.config.Cert != nil {
		hash = s.config.Cert.FingerprintBase64()
	} else {
		cert, err := s.tlsCertificate()
		if err != nil {
			slog.Error("failed to get TLS certificate", "error", err)
			writeError(w, http.StatusInternalServerError, "no TLS certificate available")
			return
		}
		fp := sha256.Sum256(cert.Certificate[0])
		hash = base64.StdEncoding.EncodeToString(fp[:])
	}
	writeJSON(w, http.StatusOK, certHashResponse{
		Hash:    hash,
		Addr:    s.config.Addr,
		Trusted: s.config.ExternalCert,
	})
}

func (s *Server) tlsCertificate() (*tls.Certificate, error) {
	tc := s.config.TLSConfig
	if tc.GetCertificate != nil {
		cert, err := tc.GetCertificate(&tls.ClientHelloInfo{})
		if err != nil {
			return nil, err
		}
		if cert != nil {
			return cert, nil
		}
	}
	if len(tc.Certificates) > 0 {
		return &tc.Certificates[0], nil
	}
	return nil, errors.New("no certificate in TLSConfig")
}

func (s *Server) handleSRTPullOptions(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.WriteHeader(http.StatusNoContent)
}

// SECURITY: The SRT pull endpoint accepts arbitrary addresses, which could be
// used for SSRF if exposed to untrusted clients. In production, this endpoint
// should be restricted to authenticated operators or internal networks.
func (s *Server) handleSRTPullList(w http.ResponseWriter, _ *http.Request) {
	if s.config.SRTList == nil {
		writeJSON(w, http.StatusOK, []SRTPullInfo{})
		return
	}
	writeJSON(w, http.StatusOK, s.config.SRTList())
}

func (s *Server) handleSRTPullCreate(w http.ResponseWriter, r *http.Request) {
	if s.config.SRTPull == nil {
		writeError(w, http.StatusNotImplemented, "SRT pull not configured")
		return
	}
	var req struct {
		Address   string `json:"address"`
		StreamKey string `json:"streamKey"`
		StreamID  string `json:"streamId,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Address == "" || req.StreamKey == "" {
		writeError(w, http.StatusBadRequest, "address and streamKey are required")
		return
	}
	if err := s.config.SRTPull(req.Address, req.StreamKey, req.StreamID); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"status": "pulling", "streamKey": req.StreamKey})
}

func (s *Server) handleSRTPullStop(w http.ResponseWriter, r *http.Request) {
	if s.config.SRTStop == nil {
		writeError(w, http.StatusNotImplemented, "SRT pull not configured")
		return
	}
	streamKey := r.URL.Query().Get("streamKey")
	if streamKey == "" {
		writeError(w, http.StatusBadRequest, "streamKey query parameter required")
		return
	}
	if err := s.config.SRTStop(streamKey); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "stopped", "streamKey": streamKey})
}
