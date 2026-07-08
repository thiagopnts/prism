package moqclient

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	webtransport "github.com/quic-go/webtransport-go"
)

// uniStreamAcceptBuffer bounds how many accepted server uni-streams may sit
// buffered between the accept pump (AcceptUniStream) and the demux loop that
// reads their subgroup header, before the pump drops (CancelRead) the overflow.
// Each buffered entry is just a stream handle — the bytes behind it are bounded
// by the connection receive window, not this count — so it is sized generously
// to ride out a reconnect burst where the origin re-opens streams for every
// subscribed track (10+) plus their initial GOPs faster than the demux loop
// drains them. At ~50 Mbps across many tracks a shallow buffer turns that burst
// into dropped GOPs; the demux loop only reads a header per stream so it drains
// this channel far faster than it fills in steady state.
const uniStreamAcceptBuffer = 256

// DialConfig holds the parameters for dialing an upstream prism distribution server.
type DialConfig struct {
	// ServerAddr is "host:port" of the upstream.
	ServerAddr string
	// Path is the request path, e.g. "/moq?stream=<streamKey>".
	Path string
	// TLSConfig for the QUIC handshake. If nil, system roots are used.
	TLSConfig *tls.Config
	// QUICConfig is optional; nil uses reasonable defaults.
	QUICConfig *quic.Config
}

// Conn is a live WebTransport client connection ready for MoQ use.
type Conn struct {
	// Control is the bidirectional WebTransport stream used for MoQ control messages.
	Control io.ReadWriter
	// DataStreams receives each server-opened unidirectional WebTransport stream,
	// one per MoQ subgroup. webtransport-go has already consumed the WebTransport
	// uni-stream header (stream-type + session-ID), so each reader starts at the
	// MoQ subgroup header.
	DataStreams <-chan io.Reader

	closeFn func()
}

// Close closes the underlying QUIC connection.
func (c *Conn) Close() error {
	if c.closeFn != nil {
		c.closeFn()
	}
	return nil
}

// TLSConfigFromCertHash builds a *tls.Config that verifies the server certificate
// against a single SHA-256 fingerprint (base64-encoded). This is how prism
// distributes its self-signed QUIC certs via /api/cert-hash.
func TLSConfigFromCertHash(base64Hash string) (*tls.Config, error) {
	return TLSConfigFromCertHashes([]string{base64Hash})
}

// TLSConfigFromCertHashes builds a *tls.Config that accepts any peer cert
// whose SHA-256 fingerprint matches any of the supplied base64-encoded hashes.
// Used when verifying against a centralized cert registry that contains the
// active certs for an entire cluster.
func TLSConfigFromCertHashes(base64Hashes []string) (*tls.Config, error) {
	if len(base64Hashes) == 0 {
		return nil, fmt.Errorf("at least one cert hash is required")
	}
	pinned := make([][sha256.Size]byte, 0, len(base64Hashes))
	for _, h := range base64Hashes {
		if h == "" {
			continue
		}
		hashBytes, err := base64.StdEncoding.DecodeString(h)
		if err != nil {
			return nil, fmt.Errorf("decode cert hash %q: %w", h, err)
		}
		if len(hashBytes) != sha256.Size {
			return nil, fmt.Errorf("cert hash must be %d bytes, got %d", sha256.Size, len(hashBytes))
		}
		var p [sha256.Size]byte
		copy(p[:], hashBytes)
		pinned = append(pinned, p)
	}
	if len(pinned) == 0 {
		return nil, fmt.Errorf("at least one non-empty cert hash is required")
	}

	return &tls.Config{
		// InsecureSkipVerify bypasses the normal chain validation; we rely on
		// the explicit SHA-256 pin instead.
		InsecureSkipVerify: true, //nolint:gosec
		VerifyConnection: func(cs tls.ConnectionState) error {
			for _, cert := range cs.PeerCertificates {
				fp := sha256.Sum256(cert.Raw)
				for _, p := range pinned {
					if fp == p {
						return nil
					}
				}
			}
			return fmt.Errorf("server cert does not match any of %d pinned hashes", len(pinned))
		},
	}, nil
}

// Dial establishes a WebTransport client connection to a prism distribution
// server and returns a Conn ready for the MoQ handshake.
//
// prism's distribution server speaks standard WebTransport (quic-go's
// webtransport-go), so we use the matching client Dialer rather than a
// hand-rolled HTTP/3 upgrade:
//  1. Dial QUIC + HTTP/3 and perform the extended-CONNECT WebTransport upgrade.
//  2. Open a bidirectional stream as the MoQ control channel.
//  3. Funnel server-initiated unidirectional streams (one per MoQ subgroup)
//     into Conn.DataStreams.
func Dial(ctx context.Context, cfg DialConfig) (*Conn, error) {
	dialer := &webtransport.Dialer{
		TLSClientConfig: buildTLSConfig(cfg.TLSConfig),
		QUICConfig:      buildQUICConfig(cfg.QUICConfig),
	}

	// cfg.Path may include a query string ("/moq?stream=foo"); the Dialer
	// parses the full URL, so the "?" lands in the request's query as prism's
	// http.ServeMux expects.
	urlStr := fmt.Sprintf("https://%s%s", cfg.ServerAddr, cfg.Path)
	resp, session, err := dialer.Dial(ctx, urlStr, nil)
	if err != nil {
		dialer.Close()
		return nil, fmt.Errorf("webtransport dial: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		session.CloseWithError(0, "connect rejected")
		dialer.Close()
		return nil, fmt.Errorf("CONNECT rejected: %d %s", resp.StatusCode, resp.Status)
	}

	// MoQ control channel: a client-initiated bidirectional stream.
	// webtransport-go writes the WebTransport stream framing; the MoQ control
	// protocol rides on top.
	control, err := session.OpenStreamSync(ctx)
	if err != nil {
		session.CloseWithError(0, "open control stream failed")
		dialer.Close()
		return nil, fmt.Errorf("open control stream: %w", err)
	}

	// Pump server-initiated unidirectional streams into DataStreams. Each one
	// carries a single MoQ subgroup; webtransport-go has already consumed the
	// uni-stream header (type + session-ID), so each reader starts at the MoQ
	// subgroup header.
	dataStreams := make(chan io.Reader, uniStreamAcceptBuffer)
	go func() {
		defer close(dataStreams)
		var dropped atomic.Int64
		for {
			str, err := session.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			select {
			case dataStreams <- str:
			default:
				// Backpressure: drop rather than stall the accept loop; the
				// ingester simply sees fewer objects until it catches up. A
				// dropped subgroup is a lost GOP/object, so surface it (rate
				// limited) — sustained drops are a likely cause of playback
				// hitches downstream.
				str.CancelRead(0)
				if n := dropped.Add(1); n%50 == 1 {
					slog.Warn("moq pull: dropping incoming subgroup (consumer backpressure)",
						"dropped_total", n)
				}
			}
		}
	}()

	return &Conn{
		Control:     control,
		DataStreams: dataStreams,
		closeFn: func() {
			control.Close()
			session.CloseWithError(0, "")
			dialer.Close()
		},
	}, nil
}

// buildTLSConfig merges the provided TLS config with the required HTTP/3 ALPN.
func buildTLSConfig(base *tls.Config) *tls.Config {
	var cfg *tls.Config
	if base != nil {
		cfg = base.Clone()
	} else {
		cfg = &tls.Config{}
	}
	cfg.NextProtos = []string{"h3"}
	return cfg
}

// buildQUICConfig fills in keepalive / idle-timeout defaults for the pull
// connection. The ingest pull is long-lived and receive-mostly; without a
// keepalive period quic-go never sends PINGs, so a brief gap in upstream
// delivery lets the idle timeout (30s by default) tear the connection down,
// forcing a reconnect that shows up as a playback hitch. A keepalive well under
// the idle timeout keeps the connection up across such gaps.
func buildQUICConfig(base *quic.Config) *quic.Config {
	var cfg *quic.Config
	if base != nil {
		cfg = base.Clone()
	} else {
		cfg = &quic.Config{}
	}
	if cfg.KeepAlivePeriod == 0 {
		cfg.KeepAlivePeriod = 10 * time.Second
	}
	if cfg.MaxIdleTimeout == 0 {
		cfg.MaxIdleTimeout = 60 * time.Second
	}
	// Flow-control receive windows. These are the values WE advertise to the
	// upstream, so they bound how much it may push before blocking on MAX_DATA /
	// MAX_STREAM_DATA. quic-go's defaults (initial connection window ~768KB,
	// auto-tuning slowly toward a 15MB cap) are far too small for a sustained
	// live-video pull: the initial connection window drains in well under a
	// second of ~30fps video, the upstream's write loop blocks on MAX_DATA — which
	// stalls every track on the shared connection at once, video and audio alike —
	// and delivery arrives as a stall/burst sawtooth. A large INITIAL connection
	// window is the load-bearing setting: it skips the slow auto-tune ramp so the
	// upstream is never throttled in the first place.
	if cfg.InitialStreamReceiveWindow == 0 {
		cfg.InitialStreamReceiveWindow = 6 << 20 // 6 MB
	}
	if cfg.MaxStreamReceiveWindow == 0 {
		cfg.MaxStreamReceiveWindow = 16 << 20 // 16 MB
	}
	if cfg.InitialConnectionReceiveWindow == 0 {
		cfg.InitialConnectionReceiveWindow = 16 << 20 // 16 MB
	}
	if cfg.MaxConnectionReceiveWindow == 0 {
		cfg.MaxConnectionReceiveWindow = 64 << 20 // 64 MB
	}
	// Headroom for prism's per-GOP (video) and per-frame (caption / stats)
	// uni-streams so a transient retirement lag never hits the default 100-stream
	// ceiling. Secondary to the flow-control windows above.
	if cfg.MaxIncomingUniStreams == 0 {
		cfg.MaxIncomingUniStreams = 1024
	}
	// WebTransport runs over QUIC datagrams AND requires the reliable
	// stream-reset-with-partial-delivery extension; webtransport-go's Dialer
	// validates both and errors before dialing if either is unset. (A nil config
	// gets both for free from the Dialer's own default, but once we supply one we
	// must set them ourselves.)
	cfg.EnableDatagrams = true
	cfg.EnableStreamResetPartialDelivery = true
	return cfg
}
