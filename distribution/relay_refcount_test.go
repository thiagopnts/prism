package distribution

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeTrackPuller records Subscribe / Unsubscribe / EnsureStarted / Stop calls so
// the refcount and connection-lifecycle logic can be exercised without a real
// upstream connection. When relay is set it mimics the real puller's
// EnsureRawTrack so the raw fan-out registry is primed. It satisfies upstreamPuller.
type fakeTrackPuller struct {
	relay *Relay

	mu      sync.Mutex
	sub     map[string]int
	unsub   map[string]int
	started int
	stopped int
}

var _ upstreamPuller = (*fakeTrackPuller)(nil)

func newFakePuller() *fakeTrackPuller {
	return &fakeTrackPuller{sub: map[string]int{}, unsub: map[string]int{}}
}

func (f *fakeTrackPuller) Subscribe(name string) {
	if f.relay != nil {
		f.relay.EnsureRawTrack(name, replayPolicyForTrack(name))
	}
	f.mu.Lock()
	f.sub[name]++
	f.mu.Unlock()
}

func (f *fakeTrackPuller) Unsubscribe(name string) {
	f.mu.Lock()
	f.unsub[name]++
	f.mu.Unlock()
}

func (f *fakeTrackPuller) EnsureStarted(context.Context) {
	f.mu.Lock()
	f.started++
	f.mu.Unlock()
}

func (f *fakeTrackPuller) Stop() {
	f.mu.Lock()
	f.stopped++
	f.mu.Unlock()
}

func (f *fakeTrackPuller) startedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started
}

func (f *fakeTrackPuller) stoppedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stopped
}

func (f *fakeTrackPuller) subCount(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sub[name]
}

func (f *fakeTrackPuller) unsubCount(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unsub[name]
}

// newRelayedRelay builds a relay wired to puller with the given grace applied to
// both the per-track unsubscribe and the connection idle-stop, marking it as a
// relayed (moq→moq) stream.
func newRelayedRelay(puller upstreamPuller, grace time.Duration) *Relay {
	r := NewRelay()
	r.graceDur = grace
	r.idleGraceDur = grace
	r.attachPuller(puller, context.Background())
	return r
}

// eventually polls cond until it is true or the timeout elapses.
func eventually(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestAcquireTrackSubscribesOnce(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, time.Hour) // long grace: no unsubscribe during the test

	r.AcquireTrack("video")
	r.AcquireTrack("video")
	r.AcquireTrack("video")

	if got := p.subCount("video"); got != 1 {
		t.Fatalf("subscribe count = %d, want 1 (refcount should subscribe once)", got)
	}
	if got := p.unsubCount("video"); got != 0 {
		t.Fatalf("unsubscribe count = %d, want 0", got)
	}
}

func TestReleaseTrackGraceUnsubscribes(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, 20*time.Millisecond)

	r.AcquireTrack("video")
	if p.subCount("video") != 1 {
		t.Fatalf("subscribe count = %d, want 1", p.subCount("video"))
	}

	r.ReleaseTrack("video")
	// Not unsubscribed immediately — grace must elapse first.
	if got := p.unsubCount("video"); got != 0 {
		t.Fatalf("unsubscribe count = %d immediately after release, want 0 (grace)", got)
	}

	eventually(t, time.Second, func() bool { return p.unsubCount("video") == 1 })
}

func TestReacquireDuringGraceCancelsUnsubscribe(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, 50*time.Millisecond)

	r.AcquireTrack("video")
	r.ReleaseTrack("video") // starts the grace timer
	r.AcquireTrack("video") // returns within grace → cancels the timer

	// Wait well past the original grace window; no unsubscribe should fire and no
	// second subscribe should have been issued (the track stayed subscribed).
	time.Sleep(120 * time.Millisecond)
	if got := p.unsubCount("video"); got != 0 {
		t.Fatalf("unsubscribe count = %d, want 0 (re-acquire within grace should cancel)", got)
	}
	if got := p.subCount("video"); got != 1 {
		t.Fatalf("subscribe count = %d, want 1 (re-acquire within grace needs no new subscribe)", got)
	}
}

func TestMultiViewerRefcount(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, 20*time.Millisecond)

	r.AcquireTrack("video") // viewer A
	r.AcquireTrack("video") // viewer B
	if p.subCount("video") != 1 {
		t.Fatalf("subscribe count = %d, want 1", p.subCount("video"))
	}

	r.ReleaseTrack("video") // A leaves; B still watching
	time.Sleep(60 * time.Millisecond)
	if got := p.unsubCount("video"); got != 0 {
		t.Fatalf("unsubscribe count = %d while a viewer remains, want 0", got)
	}

	r.ReleaseTrack("video") // B leaves → grace → unsubscribe
	eventually(t, time.Second, func() bool { return p.unsubCount("video") == 1 })
}

func TestAcquireDistinctTracksIndependent(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, time.Hour)

	r.AcquireTrack("video")
	r.AcquireTrack("audio0")
	r.AcquireTrack("captions")

	if p.subCount("video") != 1 || p.subCount("audio0") != 1 || p.subCount("captions") != 1 {
		t.Fatalf("expected one subscribe per track, got video=%d audio0=%d captions=%d",
			p.subCount("video"), p.subCount("audio0"), p.subCount("captions"))
	}
}

func TestResubscribeAfterFullReleaseCycle(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, 15*time.Millisecond)

	r.AcquireTrack("video")
	r.ReleaseTrack("video")
	eventually(t, time.Second, func() bool { return p.unsubCount("video") == 1 })

	// A new viewer after the track fully unsubscribed must subscribe again.
	r.AcquireTrack("video")
	if got := p.subCount("video"); got != 2 {
		t.Fatalf("subscribe count = %d, want 2 (resubscribe after full release)", got)
	}
}

func TestOriginRelayRefcountNoop(t *testing.T) {
	r := NewRelay() // no puller → origin relay

	if r.isRelayed() {
		t.Fatal("origin relay reported as relayed")
	}
	// Must not panic and must do nothing.
	r.AcquireTrack("video")
	r.ReleaseTrack("video")
	r.ReleaseTrack("video")
}

func TestReleaseUnknownTrackNoop(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, time.Hour)

	// Release of a never-acquired track must be a no-op (no negative refs, no
	// spurious unsubscribe).
	r.ReleaseTrack("video")
	if got := p.unsubCount("video"); got != 0 {
		t.Fatalf("unsubscribe count = %d for never-acquired track, want 0", got)
	}

	// And an over-release after a clean cycle must not drive refs negative.
	r.AcquireTrack("video")
	r.ReleaseTrack("video")
	r.ReleaseTrack("video") // extra release
	// No panic and the track is not double-counted.
}

func TestConcurrentAcquireRelease(t *testing.T) {
	p := newFakePuller()
	r := newRelayedRelay(p, 5*time.Millisecond)

	const workers = 50
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.AcquireTrack("video")
				r.ReleaseTrack("video")
			}
		}()
	}
	wg.Wait()

	// After everything drains, the track must converge to unsubscribed: every
	// subscribe is balanced by an unsubscribe (the last release's grace fires).
	eventually(t, 2*time.Second, func() bool {
		return p.subCount("video") == p.unsubCount("video") && p.subCount("video") >= 1
	})

	// And no refcount underflow left the state wedged: a fresh acquire still works.
	r.AcquireTrack("video")
	r.trackMu.Lock()
	refs := r.tracks["video"].refs
	r.trackMu.Unlock()
	if refs != 1 {
		t.Fatalf("refs after final acquire = %d, want 1", refs)
	}
}
