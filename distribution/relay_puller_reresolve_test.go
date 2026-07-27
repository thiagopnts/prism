package distribution

import (
	"context"
	"testing"
)

func newTestPuller(cfg UpstreamConfig) *RelayPuller {
	return NewRelayPuller(NewRelay(), cfg)
}

func TestApplyReResolveChangedHostAppliesUpstream(t *testing.T) {
	p := newTestPuller(UpstreamConfig{Addr: "a:1", StreamKey: "feedX", CertHashes: []string{"h1"}})
	p.reResolve = func(_ context.Context) (UpstreamConfig, bool) {
		return UpstreamConfig{Addr: "b:2", StreamKey: "feedX", CertHashes: []string{"h2"}}, true
	}
	if keep := p.applyReResolve(context.Background()); !keep {
		t.Fatal("changed host must keep the puller going (keep=true)")
	}
	if got := pullerCfg(p).Addr; got != "b:2" {
		t.Fatalf("addr after re-resolve = %q, want b:2 (SetUpstream should apply it)", got)
	}
}

func TestApplyReResolveSameHostIsNoop(t *testing.T) {
	p := newTestPuller(UpstreamConfig{Addr: "a:1", StreamKey: "feedX", CertHashes: []string{"h1"}})
	p.reResolve = func(_ context.Context) (UpstreamConfig, bool) {
		return UpstreamConfig{Addr: "a:1", StreamKey: "feedX", CertHashes: []string{"h1"}}, true
	}
	if keep := p.applyReResolve(context.Background()); !keep {
		t.Fatal("same host must keep the puller going (keep=true)")
	}
	if got := pullerCfg(p).Addr; got != "a:1" {
		t.Fatalf("addr = %q, want a:1 (unchanged)", got)
	}
}

func TestApplyReResolveDeclineEvicts(t *testing.T) {
	p := newTestPuller(UpstreamConfig{Addr: "a:1", StreamKey: "feedX"})
	evicted := false
	p.reResolve = func(_ context.Context) (UpstreamConfig, bool) { return UpstreamConfig{}, false }
	p.onGone = func() { evicted = true }
	if keep := p.applyReResolve(context.Background()); keep {
		t.Fatal("decline must stop the puller (keep=false)")
	}
	if !evicted {
		t.Fatal("decline must call onGone to evict the stream")
	}
}
