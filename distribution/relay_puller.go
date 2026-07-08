package distribution

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
	"github.com/zsiec/prism/moq"
	"github.com/zsiec/prism/moqclient"
)

// RelayPuller pulls a stream from an upstream prism distribution server as a MoQ
// subscriber and re-serves it verbatim through a local Relay. It is the moq→moq
// half of the relay: it owns the upstream connection lifecycle (dial, handshake,
// reconnect with backoff), fetches the catalog once and forwards it unchanged via
// Relay.SetCatalog, and subscribes to media tracks lazily — only those that a
// downstream viewer is currently watching, driven by Relay.AcquireTrack /
// ReleaseTrack through the trackPuller interface.
//
// It deliberately does no decoding, transcoding, codec inspection, or catalog
// synthesis: each upstream subgroup object is forwarded byte-for-byte (group,
// subgroup and object IDs, the extension block, and the payload) via
// Relay.BroadcastObject, which fans it out to downstream viewers (see relayed.go).
type RelayPuller struct {
	relay *Relay
	cfg   UpstreamConfig
	log   *slog.Logger

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	// connCancel cancels the current connection attempt (not the whole run loop),
	// so SetUpstream can force a reconnect against a changed addr / cert set. nil
	// while disconnected. Guarded by mu.
	connCancel context.CancelFunc
	// desired is the set of track names downstream viewers currently want. It is
	// the source of truth re-applied on every (re)connection.
	desired map[string]bool
	// live is the current connection, or nil while disconnected.
	live *liveConn

	reconnects atomic.Int64
}

// UpstreamConfig identifies the upstream prism stream a RelayPuller pulls from.
type UpstreamConfig struct {
	// Addr is the upstream prism distribution server "host:port".
	Addr string
	// StreamKey is the prism stream key to subscribe to upstream.
	StreamKey string
	// CertHashes are base64 SHA-256 fingerprints to pin the upstream's QUIC cert
	// against; empty means use the system roots.
	CertHashes []string
	// MaxBackoff caps the reconnect wait. Default: 30s.
	MaxBackoff time.Duration
	// Logger; nil uses slog.Default.
	Logger *slog.Logger
}

// liveConn holds the per-connection mutable state. reqByTrack maps a track name
// to the request ID we subscribed it with on this connection, so Unsubscribe can
// reference it and a concurrent re-subscribe of the same track is suppressed. It
// is guarded by RelayPuller.mu — the same lock that guards desired — so a
// subscribe/unsubscribe decision and the desired-set membership it depends on
// stay atomic (see the reconnect race note in runOnce).
type liveConn struct {
	sess       *moqclient.Session
	pending    *pendingReqs
	reqByTrack map[string]uint64
}

const (
	// catalogTrack is the reserved track name carrying the verbatim catalog. It
	// is subscribed automatically on every connection and re-served via
	// Relay.SetCatalog rather than forwarded as a raw media track.
	catalogTrack = "catalog"
	// aliasWaitTimeout bounds how long a freshly accepted data stream waits for
	// its track alias to be resolved by a SUBSCRIBE_OK before being dropped.
	aliasWaitTimeout = 5 * time.Second
	// trackReaderQueue bounds the per-track backlog of accepted subgroup streams
	// awaiting their serial reader. On overflow the stream is dropped (CancelRead,
	// returning its flow-control credit) rather than stalling the shared demux
	// loop. It absorbs jitter between the demux loop and a track's serial reader —
	// including the brief per-connection window where the reader is still waiting
	// for its alias to resolve — not seconds of media: the connection receive
	// window is the real byte backstop, and a live relay must not turn into a
	// multi-second buffer. Sized for comfortable headroom at ~50 Mbps (esp. for
	// high-object-rate caption / stats tracks and the reconnect burst) while the
	// reader keeps queue depth near zero in steady state.
	trackReaderQueue = 32
)

// NewRelayPuller creates a puller bound to relay. It does not connect; call
// EnsureStarted to bring the upstream connection up (lazily, on first viewer).
func NewRelayPuller(relay *Relay, cfg UpstreamConfig) *RelayPuller {
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	// Store the cert hashes sorted+deduped so SetUpstream's set comparison is a
	// cheap slices.Equal and a reorder from the central endpoint is a no-op.
	cfg.CertHashes = sortedCopyHashes(cfg.CertHashes)
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &RelayPuller{
		relay:   relay,
		cfg:     cfg,
		log:     log.With("relay_stream", cfg.StreamKey, "upstream", cfg.Addr),
		desired: make(map[string]bool),
	}
}

// EnsureStarted starts the pull lifecycle goroutine once, deriving its context
// from ctx (which should outlive any single viewer — the connection is shared).
// Idempotent: subsequent calls while running are no-ops.
func (p *RelayPuller) EnsureStarted(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return
	}
	p.started = true
	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	go p.run(runCtx)
}

// Stop tears the pull lifecycle down and resets so a later EnsureStarted can
// restart it (e.g. when viewers return after the idle grace closed the conn).
func (p *RelayPuller) Stop() {
	p.mu.Lock()
	cancel := p.cancel
	p.cancel = nil
	p.started = false
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// namespace is the MoQ namespace tuple for this upstream stream.
func (p *RelayPuller) namespace() []string {
	return []string{"prism", p.cfg.StreamKey}
}

// Subscribe (trackPuller) ensures the upstream is subscribed to trackName,
// registering the track's replay policy and sending a SUBSCRIBE if a connection
// is live. Idempotent per connection. Called by Relay.AcquireTrack on a 0→1
// viewer transition.
func (p *RelayPuller) Subscribe(trackName string) {
	p.relay.EnsureRawTrack(trackName, replayPolicyForTrack(trackName))

	p.mu.Lock()
	defer p.mu.Unlock()
	p.desired[trackName] = true
	p.sendSubscribeLocked(trackName)
}

// Unsubscribe (trackPuller) drops trackName from the desired set and sends an
// UNSUBSCRIBE if a connection is live. Called by Relay.ReleaseTrack once a
// track's idle grace expires.
func (p *RelayPuller) Unsubscribe(trackName string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.desired, trackName)
	p.sendUnsubscribeLocked(trackName)
}

// SetUpstream updates the dial target and cert-hash trust set. If neither
// changed it is a no-op and returns false. Otherwise it stores the new values
// and, if a connection is currently up, cancels it so the run loop reconnects
// against the new upstream (it picks up the new addr/hashes from the snapshot at
// the top of runOnce). The hash slice is compared as an unordered set — a reorder
// from the central endpoint does not trigger a reconnect.
func (p *RelayPuller) SetUpstream(addr string, certHashes []string) bool {
	next := sortedCopyHashes(certHashes)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cfg.Addr == addr && slices.Equal(p.cfg.CertHashes, next) {
		return false
	}
	p.cfg.Addr = addr
	p.cfg.CertHashes = next
	if p.connCancel != nil {
		p.connCancel()
	}
	return true
}

// sortedCopyHashes returns a sorted, deduplicated copy of the cert hashes, or nil
// for an empty input, so two hash sets that differ only in order or duplicates
// compare equal under slices.Equal.
func sortedCopyHashes(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := append([]string(nil), in...)
	slices.Sort(out)
	return slices.Compact(out)
}

// sendSubscribeLocked issues an upstream SUBSCRIBE for trackName if a connection
// is live and it is not already subscribed on it. The caller holds p.mu and the
// control write happens under it, so this is ordered with respect to
// sendUnsubscribeLocked and the reconnect re-subscribe loop: a track whose grace
// expired during a reconnect is either still in desired here (and re-subscribed,
// then unsubscribed once the grace's Unsubscribe runs) or already removed (and
// skipped) — it can never be left subscribed upstream with no viewer.
func (p *RelayPuller) sendSubscribeLocked(trackName string) {
	if p.live == nil {
		return
	}
	if _, already := p.live.reqByTrack[trackName]; already {
		return
	}
	reqID, err := p.live.sess.Subscribe(p.namespace(), trackName)
	if err != nil {
		p.log.Debug("relay pull: upstream subscribe failed", "track", trackName, "error", err)
		return
	}
	p.live.reqByTrack[trackName] = reqID
	p.live.pending.set(reqID, trackName)
}

// sendUnsubscribeLocked issues an upstream UNSUBSCRIBE for trackName if it is
// currently subscribed on the live connection. Caller holds p.mu.
func (p *RelayPuller) sendUnsubscribeLocked(trackName string) {
	if p.live == nil {
		return
	}
	reqID, ok := p.live.reqByTrack[trackName]
	if !ok {
		return
	}
	delete(p.live.reqByTrack, trackName)
	if err := p.live.sess.Unsubscribe(reqID); err != nil {
		p.log.Debug("relay pull: upstream unsubscribe failed", "track", trackName, "error", err)
	}
}

// run is the reconnect loop: it keeps a single connection alive, backing off
// exponentially (1s → MaxBackoff) after a failure and resetting on success.
func (p *RelayPuller) run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := p.runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			p.reconnects.Add(1)
			p.log.Warn("relay pull disconnected; reconnecting",
				"error", err, "backoff", backoff, "reconnects", p.reconnects.Load())
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		if err != nil {
			backoff *= 2
			if backoff > p.cfg.MaxBackoff {
				backoff = p.cfg.MaxBackoff
			}
		} else {
			backoff = time.Second
		}
	}
}

// runOnce handles a single connection lifetime: dial, handshake, subscribe the
// catalog and every desired track, then demux data streams until the connection
// drops or ctx is cancelled.
func (p *RelayPuller) runOnce(ctx context.Context) error {
	// Snapshot the mutable upstream config (Addr / CertHashes) under the lock;
	// SetUpstream can change them concurrently. StreamKey is immutable.
	p.mu.Lock()
	addr := p.cfg.Addr
	certHashes := p.cfg.CertHashes
	p.mu.Unlock()

	var tlsConf *tls.Config
	if len(certHashes) > 0 {
		var err error
		tlsConf, err = moqclient.TLSConfigFromCertHashes(certHashes)
		if err != nil {
			return fmt.Errorf("tls config: %w", err)
		}
	}

	path := fmt.Sprintf("/moq?stream=%s", p.cfg.StreamKey)
	conn, err := moqclient.Dial(ctx, moqclient.DialConfig{
		ServerAddr: addr,
		Path:       path,
		TLSConfig:  tlsConf,
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	sess := moqclient.NewSession(conn.Control)
	if err := sess.Handshake(path); err != nil {
		conn.Close()
		return fmt.Errorf("handshake: %w", err)
	}
	p.log.Info("relay pull handshake complete")

	aliases := newTrackAliasMap()
	pending := newPendingReqs()
	live := &liveConn{sess: sess, pending: pending, reqByTrack: make(map[string]uint64)}

	// Subscribe the catalog first so it lands before any media objects.
	catReqID, err := sess.Subscribe(p.namespace(), catalogTrack)
	if err != nil {
		conn.Close()
		return fmt.Errorf("subscribe catalog: %w", err)
	}
	pending.set(catReqID, catalogTrack)

	// Publish as the live connection and re-subscribe every track viewers are
	// currently watching, all under p.mu so the re-subscribe is atomic with the
	// desired set: a track released concurrently (its grace expiring mid-reconnect)
	// is either still desired here and re-subscribed, or already removed and
	// skipped — it can never be left subscribed upstream with no viewer. Calls to
	// Subscribe/Unsubscribe arriving after this block go through the same p.mu.
	p.mu.Lock()
	p.live = live
	for name := range p.desired {
		p.sendSubscribeLocked(name)
	}
	p.mu.Unlock()

	connCtx, connCancel := context.WithCancel(ctx)
	// Publish connCancel so SetUpstream can drop this connection on a config change
	// and the run loop reconnects against the new upstream.
	p.mu.Lock()
	p.connCancel = connCancel
	p.mu.Unlock()

	ctlErr := make(chan error, 1)
	go func() { ctlErr <- p.controlLoop(connCtx, sess, pending, aliases) }()

	readers := newTrackReaderSet(connCtx, p, aliases)
	p.acceptStreams(connCtx, conn, readers)

	// Teardown order matters: cancel the control loop, then close the connection
	// (which resets the accepted uni-streams so the per-track readers unblock
	// from ReadObject), then drain the readers, then drop the live connection.
	p.mu.Lock()
	p.connCancel = nil
	p.mu.Unlock()
	connCancel()
	conn.Close()
	readers.shutdown()
	p.clearLive(live)

	select {
	case e := <-ctlErr:
		return e
	default:
		return nil
	}
}

// clearLive drops the live connection if it is still the given one (a no-op if a
// newer connection has already replaced it).
func (p *RelayPuller) clearLive(live *liveConn) {
	p.mu.Lock()
	if p.live == live {
		p.live = nil
	}
	p.mu.Unlock()
}

// controlLoop reads control messages and resolves track aliases as SUBSCRIBE_OKs
// arrive. Returns when the control stream errors or the server sends GOAWAY.
func (p *RelayPuller) controlLoop(ctx context.Context, sess *moqclient.Session, pending *pendingReqs, aliases *trackAliasMap) error {
	for ctx.Err() == nil {
		msgType, payload, err := sess.ReadMessage()
		if err != nil {
			return fmt.Errorf("read control: %w", err)
		}
		switch msgType {
		case moq.MsgSubscribeOK:
			reqID, alias, perr := moqclient.ParseSubscribeOK(payload)
			if perr != nil {
				p.log.Warn("relay pull: parse SUBSCRIBE_OK", "error", perr)
				continue
			}
			name, ok := pending.take(reqID)
			if !ok {
				p.log.Warn("relay pull: SUBSCRIBE_OK for unknown request", "requestID", reqID)
				continue
			}
			aliases.set(alias, name)
			p.log.Debug("relay pull: subscribed", "track", name, "alias", alias)
		case moq.MsgSubscribeError:
			p.log.Warn("relay pull: upstream SUBSCRIBE_ERROR")
		case moq.MsgGoAway:
			return errors.New("upstream sent GOAWAY")
		case moq.MsgMaxRequestID:
			// peer raised our quota; fine to ignore.
		default:
			p.log.Debug("relay pull: unhandled control message", "type", fmt.Sprintf("0x%x", msgType))
		}
	}
	return ctx.Err()
}

// acceptStreams is the single demux loop: it accepts every server-opened
// unidirectional stream, reads its subgroup header, and hands the stream to the
// serial reader for its track alias. It deliberately does NOT resolve the alias
// to a track name here — that would block this shared loop (up to
// aliasWaitTimeout) on a single not-yet-resolved alias while streams for every
// already-resolved track pile up undrained, which on a 10+-track feed stalls the
// whole demux at reconnect. Instead each per-alias reader waits for its own alias
// (see trackReaderSet.loop), so a slow alias parks only its own reader. Routing
// by alias (1:1 with a track for the life of a connection) still preserves
// per-track object ordering: every stream for one alias flows through that one
// reader's queue in accept order.
func (p *RelayPuller) acceptStreams(ctx context.Context, conn *moqclient.Conn, readers *trackReaderSet) {
	for {
		select {
		case <-ctx.Done():
			return
		case stream, ok := <-conn.DataStreams:
			if !ok {
				return
			}
			r := bufio.NewReader(stream)
			hdr, err := moqclient.ReadSubgroupHeader(r)
			if err != nil {
				p.log.Debug("relay pull: subgroup header read failed", "error", err)
				dropStream(stream)
				continue
			}
			readers.dispatch(hdr.TrackAlias, headeredStream{hdr: hdr, r: r, stream: stream})
		}
	}
}

// readStream reads one upstream subgroup stream to completion, forwarding each
// object verbatim. The first object of the stream is marked StartsStream so the
// downstream writer opens a fresh uni-stream for it, mirroring the upstream's
// stream boundary exactly. The catalog stream is re-served via SetCatalog instead
// of forwarded as a media track.
func (p *RelayPuller) readStream(ctx context.Context, name string, hs headeredStream) {
	first := true
	for ctx.Err() == nil {
		obj, err := moqclient.ReadObject(hs.r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.log.Debug("relay pull: object read ended", "track", name, "error", err)
			}
			return
		}
		if name == catalogTrack {
			// obj.Payload is freshly allocated by ReadObject, safe to retain.
			p.relay.SetCatalog(obj.Payload)
			continue
		}
		p.relay.BroadcastObject(name, RawObject{
			GroupID:      hs.hdr.GroupID,
			SubgroupID:   hs.hdr.SubgroupID,
			ObjectID:     obj.ObjectID,
			Priority:     hs.hdr.Priority,
			ExtBytes:     obj.RawExtBytes,
			Payload:      obj.Payload,
			StartsStream: first,
		})
		first = false
	}
}

// replayPolicyForTrack picks the replay buffering shape for a track by name:
// video and captions begin each group with an independently decodable unit
// (keyframe / caption group) → ReplayCurrentGroup; audio rides one boundary-less
// long-lived stream → ReplayRecentRing; everything else (stats) → ReplayNone.
func replayPolicyForTrack(trackName string) RawReplayPolicy {
	switch {
	case trackName == "video" || trackName == "captions":
		return ReplayCurrentGroup
	case strings.HasPrefix(trackName, "audio"):
		return ReplayRecentRing
	default:
		return ReplayNone
	}
}

// --- per-track serial readers ---

// headeredStream pairs a subgroup header with the stream reader positioned just
// past it. stream is the raw underlying uni-stream (the *bufio.Reader wraps it);
// it is retained so a stream we drop undrained can be released via dropStream.
type headeredStream struct {
	hdr    moqclient.SubgroupHeader
	r      *bufio.Reader
	stream io.Reader
}

// dropStream releases an accepted upstream uni-stream we are not going to read.
// quic-go charges a stream's bytes against the connection receive window on
// arrival and only returns that credit (and retires the MAX_STREAMS slot) when
// the application reads the stream to completion or cancels it. Silently dropping
// an accepted-but-undrained stream therefore leaks connection-level flow-control
// credit for the whole life of the connection, which eventually blocks the
// origin's writes on MAX_DATA (stalling every track at once). CancelRead sends
// STOP_SENDING so the stream is retired and its credit reclaimed — mirroring the
// accept-pump drop in moqclient.Dial.
func dropStream(r io.Reader) {
	if c, ok := r.(interface {
		CancelRead(webtransport.StreamErrorCode)
	}); ok {
		c.CancelRead(0)
	}
}

// Compile-time guarantee that the concrete upstream uni-stream type satisfies the
// shape dropStream asserts for. A webtransport API change to CancelRead's
// signature breaks the build here rather than silently turning dropStream into a
// no-op.
var _ interface {
	CancelRead(webtransport.StreamErrorCode)
} = (*webtransport.ReceiveStream)(nil)

// trackReaderSet owns one serial reader goroutine per track alias for a single
// connection. Routing each alias's subgroup streams to a dedicated goroutine
// preserves per-track object ordering (so the downstream stream reconstruction is
// faithful) while still reading different tracks concurrently (no cross-track
// head-of-line blocking). Keying by alias rather than track name lets the demux
// loop dispatch a stream before its SUBSCRIBE_OK has resolved the alias to a
// name: the reader itself waits for the alias (see loop), so alias resolution
// never blocks the shared demux loop.
type trackReaderSet struct {
	p           *RelayPuller
	ctx         context.Context
	aliases     *trackAliasMap
	waitTimeout time.Duration

	mu      sync.Mutex
	readers map[uint64]chan headeredStream
	wg      sync.WaitGroup
}

func newTrackReaderSet(ctx context.Context, p *RelayPuller, aliases *trackAliasMap) *trackReaderSet {
	return &trackReaderSet{
		p:           p,
		ctx:         ctx,
		aliases:     aliases,
		waitTimeout: aliasWaitTimeout,
		readers:     make(map[uint64]chan headeredStream),
	}
}

// dispatch hands a stream to its alias's reader, lazily starting that reader.
// Drops the stream (returning its flow-control credit via CancelRead) if the
// alias's queue is full rather than stalling the shared demux loop.
func (s *trackReaderSet) dispatch(alias uint64, hs headeredStream) {
	s.mu.Lock()
	ch := s.readers[alias]
	if ch == nil {
		ch = make(chan headeredStream, trackReaderQueue)
		s.readers[alias] = ch
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.loop(alias, ch)
		}()
	}
	s.mu.Unlock()

	select {
	case ch <- hs:
	default:
		s.p.log.Debug("relay pull: track reader backpressure; dropping stream", "alias", alias)
		dropStream(hs.stream)
	}
}

// loop resolves the alias to a track name (waiting only this goroutine, not the
// shared demux loop) and then serially reads the streams routed to that alias in
// accept order. If the alias never resolves within waitTimeout — a stream for a
// track we never got a SUBSCRIBE_OK for — the reader keeps draining and dropping
// its queue (CancelRead, returning flow-control credit) rather than letting those
// streams leak; it exits when the connection tears down.
func (s *trackReaderSet) loop(alias uint64, in <-chan headeredStream) {
	name, ok := s.aliases.waitFor(s.ctx, alias, s.waitTimeout)
	if !ok {
		s.p.log.Debug("relay pull: unknown track alias; dropping streams", "alias", alias)
		for {
			select {
			case <-s.ctx.Done():
				return
			case hs, ok := <-in:
				if !ok {
					return
				}
				dropStream(hs.stream)
			}
		}
	}
	for {
		select {
		case <-s.ctx.Done():
			return
		case hs, ok := <-in:
			if !ok {
				return
			}
			s.p.readStream(s.ctx, name, hs)
		}
	}
}

// shutdown closes every track's queue and waits for its reader to exit. The
// caller must have already closed the connection so any reader blocked in
// ReadObject is unblocked by the stream reset.
func (s *trackReaderSet) shutdown() {
	s.mu.Lock()
	for _, ch := range s.readers {
		close(ch)
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// trackAliasMap is a goroutine-safe map of trackAlias → trackName, built up as
// SUBSCRIBE_OKs arrive.
type trackAliasMap struct {
	mu  sync.RWMutex
	m   map[uint64]string
	new chan struct{} // closed and replaced when a new entry is added
}

func newTrackAliasMap() *trackAliasMap {
	return &trackAliasMap{
		m:   make(map[uint64]string),
		new: make(chan struct{}),
	}
}

func (m *trackAliasMap) set(alias uint64, name string) {
	m.mu.Lock()
	m.m[alias] = name
	old := m.new
	m.new = make(chan struct{})
	m.mu.Unlock()
	close(old)
}

// waitFor blocks until alias is in the map, ctx is cancelled, or timeout elapses.
func (m *trackAliasMap) waitFor(ctx context.Context, alias uint64, timeout time.Duration) (string, bool) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		m.mu.RLock()
		n, ok := m.m[alias]
		ch := m.new
		m.mu.RUnlock()
		if ok {
			return n, true
		}
		select {
		case <-ch:
			// a new entry was added; retry
		case <-ctx.Done():
			return "", false
		case <-deadline.C:
			return "", false
		}
	}
}

// pendingReqs tracks subscribe requests awaiting SUBSCRIBE_OK.
type pendingReqs struct {
	mu sync.Mutex
	m  map[uint64]string
}

func newPendingReqs() *pendingReqs { return &pendingReqs{m: make(map[uint64]string)} }

func (p *pendingReqs) set(reqID uint64, name string) {
	p.mu.Lock()
	p.m[reqID] = name
	p.mu.Unlock()
}

func (p *pendingReqs) take(reqID uint64) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, ok := p.m[reqID]
	if ok {
		delete(p.m, reqID)
	}
	return n, ok
}
