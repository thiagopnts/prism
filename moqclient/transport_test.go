package moqclient

import (
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// TestBuildQUICConfig_EnablesDatagramsAndKeepalive guards the WebTransport
// requirement: a supplied QUIC config must enable datagrams (webtransport-go
// rejects the dial otherwise) and carry a keepalive so the long-lived pull
// connection survives brief gaps in upstream delivery.
func TestBuildQUICConfig_EnablesDatagramsAndKeepalive(t *testing.T) {
	cfg := buildQUICConfig(nil)
	if !cfg.EnableDatagrams {
		t.Error("WebTransport requires QUIC datagrams")
	}
	if !cfg.EnableStreamResetPartialDelivery {
		t.Error("WebTransport requires stream-reset partial delivery")
	}
	if cfg.KeepAlivePeriod != 10*time.Second {
		t.Errorf("KeepAlivePeriod = %v, want %v", cfg.KeepAlivePeriod, 10*time.Second)
	}
	if cfg.MaxIdleTimeout != 60*time.Second {
		t.Errorf("MaxIdleTimeout = %v, want %v", cfg.MaxIdleTimeout, 60*time.Second)
	}
}

// TestBuildQUICConfig_LargeReceiveWindows guards the flow-control fix: a live
// video pull must advertise large receive windows (especially the INITIAL
// connection window) so the upstream isn't throttled by MAX_DATA into a
// stall/burst sawtooth. A regression to quic-go's ~768KB default would silently
// reintroduce the periodic stalls.
func TestBuildQUICConfig_LargeReceiveWindows(t *testing.T) {
	cfg := buildQUICConfig(nil)
	if cfg.InitialConnectionReceiveWindow != uint64(16<<20) {
		t.Errorf("InitialConnectionReceiveWindow = %d, want %d (must skip the slow auto-tune ramp)",
			cfg.InitialConnectionReceiveWindow, 16<<20)
	}
	if cfg.MaxConnectionReceiveWindow != uint64(64<<20) {
		t.Errorf("MaxConnectionReceiveWindow = %d, want %d", cfg.MaxConnectionReceiveWindow, 64<<20)
	}
	if cfg.InitialStreamReceiveWindow != uint64(6<<20) {
		t.Errorf("InitialStreamReceiveWindow = %d, want %d", cfg.InitialStreamReceiveWindow, 6<<20)
	}
	if cfg.MaxStreamReceiveWindow != uint64(16<<20) {
		t.Errorf("MaxStreamReceiveWindow = %d, want %d", cfg.MaxStreamReceiveWindow, 16<<20)
	}
	if cfg.MaxIncomingUniStreams != int64(1024) {
		t.Errorf("MaxIncomingUniStreams = %d, want 1024", cfg.MaxIncomingUniStreams)
	}
}

// TestBuildQUICConfig_PreservesCallerValues confirms caller-provided fields are
// kept while the WebTransport-required flags are still forced on.
func TestBuildQUICConfig_PreservesCallerValues(t *testing.T) {
	cfg := buildQUICConfig(&quic.Config{KeepAlivePeriod: 3 * time.Second})
	if cfg.KeepAlivePeriod != 3*time.Second {
		t.Errorf("caller KeepAlivePeriod = %v, want %v (must be preserved)", cfg.KeepAlivePeriod, 3*time.Second)
	}
	if !cfg.EnableDatagrams {
		t.Error("datagrams must be forced on even with a caller config")
	}
	if !cfg.EnableStreamResetPartialDelivery {
		t.Error("partial delivery must be forced on even with a caller config")
	}
}
