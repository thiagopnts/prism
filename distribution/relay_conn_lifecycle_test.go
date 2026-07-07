package distribution

import (
	"testing"
	"time"
)

// The connection lifecycle is edge-triggered on viewer presence via
// EnsureUpstream / ReleaseUpstream, independent of the per-track refcount. These
// tests drive those edges against a recording fake puller.

func TestEnsureUpstreamStartsPullerAndHoldsConnection(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, 10*time.Millisecond)

	r.EnsureUpstream()

	if got := p.startedCount(); got == 0 {
		t.Fatalf("EnsureStarted not called; started=%d", got)
	}
	// With a viewer present the idle-stop must never fire.
	time.Sleep(40 * time.Millisecond)
	if got := p.stoppedCount(); got != 0 {
		t.Fatalf("puller stopped while a viewer was connected; stopped=%d", got)
	}
}

func TestReleaseUpstreamStopsAfterIdleGrace(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, 15*time.Millisecond)

	r.EnsureUpstream()
	r.ReleaseUpstream()

	eventually(t, time.Second, func() bool { return p.stoppedCount() == 1 })
}

func TestReacquireDuringIdleGraceCancelsStop(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, 50*time.Millisecond)

	r.EnsureUpstream()
	r.ReleaseUpstream() // starts the idle grace
	r.EnsureUpstream()  // returns before the grace fires; must cancel it

	// Past the original grace: the connection must still be up.
	time.Sleep(120 * time.Millisecond)
	if got := p.stoppedCount(); got != 0 {
		t.Fatalf("idle-stop fired despite a viewer returning during the grace; stopped=%d", got)
	}
	if got := p.startedCount(); got != 2 {
		t.Fatalf("started=%d, want 2 (one per EnsureUpstream)", got)
	}
}

func TestMultiViewerConnRefcountHoldsUntilLastLeaves(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, 15*time.Millisecond)

	r.EnsureUpstream()
	r.EnsureUpstream()

	r.ReleaseUpstream() // one viewer remains; no idle-stop
	time.Sleep(50 * time.Millisecond)
	if got := p.stoppedCount(); got != 0 {
		t.Fatalf("puller stopped with a viewer still present; stopped=%d", got)
	}

	r.ReleaseUpstream() // last viewer leaves
	eventually(t, time.Second, func() bool { return p.stoppedCount() == 1 })
}

func TestConnLifecycleStopThenRestart(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, 15*time.Millisecond)

	r.EnsureUpstream()
	r.ReleaseUpstream()
	eventually(t, time.Second, func() bool { return p.stoppedCount() == 1 })

	// A viewer returning after the connection was torn down restarts it.
	r.EnsureUpstream()
	if got := p.startedCount(); got != 2 {
		t.Fatalf("started=%d, want 2 after restart", got)
	}
}

func TestReleaseWithoutAcquireIsNoop(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, 10*time.Millisecond)

	// No matching EnsureUpstream: connRefs is already zero, so this must not
	// schedule an idle-stop nor stop the (unstarted) puller.
	r.ReleaseUpstream()
	time.Sleep(40 * time.Millisecond)
	if got := p.stoppedCount(); got != 0 {
		t.Fatalf("release without acquire stopped the puller; stopped=%d", got)
	}
	if got := p.startedCount(); got != 0 {
		t.Fatalf("release without acquire started the puller; started=%d", got)
	}
}

func TestShutdownUpstreamCancelsPendingTimers(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, time.Hour) // long grace so the timers stay pending

	r.EnsureUpstream()
	r.AcquireTrack("video")
	r.ReleaseTrack("video") // arms a per-track unsubscribe grace timer
	r.ReleaseUpstream()     // arms the connection idle-stop timer

	r.shutdownUpstream()

	if got := p.stoppedCount(); got != 1 {
		t.Fatalf("shutdownUpstream did not stop the puller; stopped=%d", got)
	}
	r.trackMu.Lock()
	idlePending := r.idleStop != nil
	ts := r.tracks["video"]
	tracePending := ts != nil && ts.grace != nil
	r.trackMu.Unlock()
	if idlePending {
		t.Fatal("idle-stop timer still pending after shutdownUpstream")
	}
	if tracePending {
		t.Fatal("per-track grace timer still pending after shutdownUpstream")
	}
}

func TestOriginRelayUpstreamLifecycleNoop(t *testing.T) {
	// An origin relay has no puller; the lifecycle calls must be safe no-ops.
	r := NewRelay()
	r.EnsureUpstream()
	r.ReleaseUpstream()
	r.shutdownUpstream()
	if r.isRelayed() {
		t.Fatal("origin relay reported as relayed")
	}
}
