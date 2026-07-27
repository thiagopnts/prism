package distribution

import (
	"context"
	"testing"
	"time"
)

func TestResolvePrimaryRelayRegistersColdRelay(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	srv.config.PrimaryUpstreamResolver = func(_ context.Context, key string) (UpstreamConfig, bool) {
		return UpstreamConfig{Addr: "10.0.0.1:6000", CertHashes: []string{"h1"}}, true
	}

	relay, ok := srv.resolvePrimaryRelay(context.Background(), "feedX")
	if !ok || relay == nil {
		t.Fatalf("resolvePrimaryRelay ok=%v relay=%v, want true/non-nil", ok, relay)
	}
	if srv.GetRelay("feedX") != relay {
		t.Fatal("resolved relay not registered under its key")
	}
	if !relay.isRelayed() {
		t.Fatal("resolved relay is not marked relayed")
	}
	// Cold: no connection until the first viewer.
	time.Sleep(20 * time.Millisecond)
	if pullerStarted(srv.streams["feedX"].puller) {
		t.Fatal("puller started before any viewer connected")
	}
	// StreamKey defaulted to the requested key; addr/hashes carried through.
	cfg := pullerCfg(srv.streams["feedX"].puller)
	if cfg.StreamKey != "feedX" || cfg.Addr != "10.0.0.1:6000" {
		t.Fatalf("puller cfg = %+v, want StreamKey=feedX Addr=10.0.0.1:6000", cfg)
	}
}

func TestResolvePrimaryRelayDeclineReturnsFalse(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	srv.config.PrimaryUpstreamResolver = func(_ context.Context, _ string) (UpstreamConfig, bool) {
		return UpstreamConfig{}, false
	}
	if relay, ok := srv.resolvePrimaryRelay(context.Background(), "gone"); ok || relay != nil {
		t.Fatalf("decline: got ok=%v relay=%v, want false/nil", ok, relay)
	}
	if srv.GetRelay("gone") != nil {
		t.Fatal("declined key must not be registered")
	}
}

func TestResolvePrimaryRelayNilResolverReturnsFalse(t *testing.T) {
	srv := newRelayTestServer(t, nil) // PrimaryUpstreamResolver unset
	if _, ok := srv.resolvePrimaryRelay(context.Background(), "feedX"); ok {
		t.Fatal("nil resolver must return ok=false")
	}
}

func TestResolvePrimaryRelayReusesRegisteredStream(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	existing := srv.RegisterRelayedUpstream("feedX", UpstreamConfig{Addr: "a:1"})
	calls := 0
	srv.config.PrimaryUpstreamResolver = func(_ context.Context, _ string) (UpstreamConfig, bool) {
		calls++
		return UpstreamConfig{Addr: "b:2"}, true
	}
	relay, ok := srv.resolvePrimaryRelay(context.Background(), "feedX")
	if !ok || relay != existing {
		t.Fatal("already-registered key must return the existing relay")
	}
	if calls != 0 {
		t.Fatalf("resolver called %d times for an already-registered key, want 0", calls)
	}
}

func TestResolvedPrimaryStreamEvictedOnReResolveDecline(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	gone := false
	srv.config.PrimaryUpstreamResolver = func(_ context.Context, _ string) (UpstreamConfig, bool) {
		if gone {
			return UpstreamConfig{}, false
		}
		return UpstreamConfig{Addr: "10.0.0.1:6000"}, true
	}

	relay, ok := srv.resolvePrimaryRelay(context.Background(), "feedX")
	if !ok || relay == nil {
		t.Fatal("initial resolve should register the stream")
	}
	puller := srv.streams["feedX"].puller

	// Simulate the puller's post-failure re-resolve after the feed disappears.
	gone = true
	if keep := puller.applyReResolve(context.Background()); keep {
		t.Fatal("re-resolve after the feed is gone must return keep=false")
	}
	if srv.GetRelay("feedX") != nil {
		t.Fatal("evicted stream must be gone from the server")
	}
}
