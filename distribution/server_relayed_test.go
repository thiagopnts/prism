package distribution

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zsiec/prism/certs"
)

func newRelayTestServer(t *testing.T, lister StreamLister) *Server {
	t.Helper()
	cert, err := certs.Generate(24 * time.Hour)
	if err != nil {
		t.Fatalf("certs.Generate: %v", err)
	}
	srv, err := NewServer(ServerConfig{Addr: ":0", Cert: cert, StreamLister: lister})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

func pullerStarted(p *RelayPuller) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.started
}

func pullerCfg(p *RelayPuller) UpstreamConfig {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg
}

func listStreams(t *testing.T, srv *Server) []StreamInfo {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/streams", nil)
	rec := httptest.NewRecorder()
	srv.APIHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var streams []StreamInfo
	if err := json.NewDecoder(rec.Body).Decode(&streams); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return streams
}

func findStream(streams []StreamInfo, key string) *StreamInfo {
	for i := range streams {
		if streams[i].Key == key {
			return &streams[i]
		}
	}
	return nil
}

func TestRegisterRelayedUpstreamColdNoEagerWork(t *testing.T) {
	srv := newRelayTestServer(t, nil)

	relay := srv.RegisterRelayedUpstream("feedA", UpstreamConfig{
		Addr:       "127.0.0.1:65000",
		CertHashes: []string{"h1", "h2"},
	})
	if relay == nil {
		t.Fatal("RegisterRelayedUpstream returned nil")
	}
	if srv.GetRelay("feedA") != relay {
		t.Fatal("GetRelay did not return the registered relay")
	}
	if !relay.isRelayed() {
		t.Fatal("registered relay is not marked relayed")
	}
	// Cold: no eager catalog fetch, no viewers, no connection started.
	if relay.Catalog() != nil {
		t.Fatal("cold relay unexpectedly has a catalog")
	}
	if relay.ViewerCount() != 0 {
		t.Fatalf("viewers = %d, want 0", relay.ViewerCount())
	}
	time.Sleep(20 * time.Millisecond)
	if pullerStarted(srv.streams["feedA"].puller) {
		t.Fatal("puller started before any viewer connected")
	}
}

func TestRegisterRelayedUpstreamDefaultsStreamKey(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	srv.RegisterRelayedUpstream("feedA", UpstreamConfig{Addr: "a:1"})
	if got := pullerCfg(srv.streams["feedA"].puller).StreamKey; got != "feedA" {
		t.Fatalf("upstream StreamKey = %q, want %q", got, "feedA")
	}
}

func TestRegisterRelayedUpstreamIdempotentUpdatesUpstream(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	r1 := srv.RegisterRelayedUpstream("feedA", UpstreamConfig{Addr: "a:1", CertHashes: []string{"h1"}})
	r2 := srv.RegisterRelayedUpstream("feedA", UpstreamConfig{Addr: "b:2", CertHashes: []string{"h2"}})
	if r1 != r2 {
		t.Fatal("re-registration returned a different relay")
	}
	cfg := pullerCfg(srv.streams["feedA"].puller)
	if cfg.Addr != "b:2" {
		t.Fatalf("upstream Addr = %q, want %q (re-register should apply the change)", cfg.Addr, "b:2")
	}
}

func TestSetRelayedUpstreamNoopOnEqualSet(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	srv.RegisterRelayedUpstream("feedA", UpstreamConfig{Addr: "a:1", CertHashes: []string{"h1", "h2"}})

	if srv.SetRelayedUpstream("feedA", "a:1", []string{"h2", "h1"}) {
		t.Fatal("reordered hash set must be a no-op (returned changed=true)")
	}
	if srv.SetRelayedUpstream("feedA", "a:1", []string{"h1", "h2", "h2"}) {
		t.Fatal("duplicate hash must be a no-op (returned changed=true)")
	}
}

func TestSetRelayedUpstreamDetectsChange(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	srv.RegisterRelayedUpstream("feedA", UpstreamConfig{Addr: "a:1", CertHashes: []string{"h1", "h2"}})

	if !srv.SetRelayedUpstream("feedA", "b:2", []string{"h1", "h2"}) {
		t.Fatal("address change should report changed=true")
	}
	if !srv.SetRelayedUpstream("feedA", "b:2", []string{"h3"}) {
		t.Fatal("hash-set change should report changed=true")
	}
}

func TestSetRelayedUpstreamUnknownOrOrigin(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	if srv.SetRelayedUpstream("nope", "a:1", nil) {
		t.Fatal("unknown stream should report changed=false")
	}
	srv.RegisterStream("origin")
	if srv.SetRelayedUpstream("origin", "a:1", nil) {
		t.Fatal("origin (non-relayed) stream should report changed=false")
	}
}

func TestUnregisterRelayedStreamRemovesAndCancels(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	relay := srv.RegisterRelayedUpstream("feedA", UpstreamConfig{Addr: "a:1"})

	srv.UnregisterRelayedStream("feedA")
	if srv.GetRelay("feedA") != nil {
		t.Fatal("relay still present after UnregisterRelayedStream")
	}
	select {
	case <-relay.upstreamCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("upstream context not cancelled after unregister")
	}
}

func TestUnregisterStreamAlsoTearsDownRelayedPuller(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	relay := srv.RegisterRelayedUpstream("feedA", UpstreamConfig{Addr: "a:1"})

	srv.UnregisterStream("feedA") // the generic unregister must also tear the puller down
	if srv.GetRelay("feedA") != nil {
		t.Fatal("relay still present after UnregisterStream")
	}
	select {
	case <-relay.upstreamCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("upstream context not cancelled by UnregisterStream")
	}
}

func TestRegisterRelayedStreamStaticPathIntact(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	catalog := []byte(`{"version":1}`)
	relay := srv.RegisterRelayedStream("static", catalog)

	if string(relay.Catalog()) != string(catalog) {
		t.Fatal("static relayed stream did not retain its catalog verbatim")
	}
	// The static path attaches no puller, so it is not a lazy-pull relayed stream.
	if relay.isRelayed() {
		t.Fatal("static RegisterRelayedStream unexpectedly attached a puller")
	}
}

func TestListStreamsIncludesIdleRelayedMinimal(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	srv.RegisterRelayedUpstream("feedA", UpstreamConfig{Addr: "a:1"})

	si := findStream(listStreams(t, srv), "feedA")
	if si == nil {
		t.Fatal("relayed stream missing from /api/streams")
	}
	if si.Protocol != "moq-relay" {
		t.Fatalf("protocol = %q, want moq-relay", si.Protocol)
	}
	if si.Viewers != 0 || si.VideoCodec != "" {
		t.Fatalf("idle relayed stream should be minimal; got %+v", *si)
	}
}

func TestListStreamsRelayedWithCatalog(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	relay := srv.RegisterRelayedUpstream("feedB", UpstreamConfig{Addr: "a:1"})

	catalog, err := json.Marshal(moqCatalog{
		Tracks: []moqCatalogTrack{
			{Name: "video", SelectionParams: moqSelectionParams{Codec: "avc1.64001f", Width: 1280, Height: 720}},
			{Name: "audio0", SelectionParams: moqSelectionParams{Codec: audioCodecAAC}},
			{Name: "audio1", SelectionParams: moqSelectionParams{Codec: audioCodecAAC}},
			{Name: "captions", SelectionParams: moqSelectionParams{Codec: "caption/v2"}},
		},
	})
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	relay.SetCatalog(catalog)

	si := findStream(listStreams(t, srv), "feedB")
	if si == nil {
		t.Fatal("relayed stream missing from /api/streams")
	}
	if si.VideoCodec != "avc1.64001f" || si.Width != 1280 || si.Height != 720 {
		t.Fatalf("video info not parsed from catalog; got %+v", *si)
	}
	if si.AudioTracks != 2 {
		t.Fatalf("audioTracks = %d, want 2", si.AudioTracks)
	}
	if !si.HasCaptions {
		t.Fatal("hasCaptions = false, want true")
	}
}

func TestRelayedStreamInfosListsRegisteredAndExcludesOrigin(t *testing.T) {
	lister := func() []StreamInfo {
		return []StreamInfo{{Key: "originFeed", Viewers: 3, Protocol: "RTP"}}
	}
	srv := newRelayTestServer(t, lister)
	srv.RegisterStream("originFeed") // origin stream — must NOT appear in the relayed list
	srv.RegisterRelayedUpstream("relayA", UpstreamConfig{Addr: "a:1"})
	relay := srv.RegisterRelayedUpstream("relayB", UpstreamConfig{Addr: "b:1"})

	catalog, err := json.Marshal(moqCatalog{
		Tracks: []moqCatalogTrack{
			{Name: "video", SelectionParams: moqSelectionParams{Codec: "avc1.64001f", Width: 1280, Height: 720}},
			{Name: "audio0", SelectionParams: moqSelectionParams{Codec: audioCodecAAC}},
		},
	})
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	relay.SetCatalog(catalog)

	infos := srv.RelayedStreamInfos()
	if findStream(infos, "originFeed") != nil {
		t.Fatal("origin stream leaked into RelayedStreamInfos")
	}
	a := findStream(infos, "relayA")
	if a == nil || a.Protocol != "moq-relay" || a.VideoCodec != "" {
		t.Fatalf("idle relayed stream relayA missing or not minimal; got %+v", a)
	}
	b := findStream(infos, "relayB")
	if b == nil || b.VideoCodec != "avc1.64001f" || b.Width != 1280 || b.AudioTracks != 1 {
		t.Fatalf("active relayed stream relayB not parsed from catalog; got %+v", b)
	}
}

func TestRelayedStreamInfosEmptyIsNonNil(t *testing.T) {
	srv := newRelayTestServer(t, nil)
	infos := srv.RelayedStreamInfos()
	if infos == nil {
		t.Fatal("RelayedStreamInfos returned nil; want empty non-nil slice")
	}
	if len(infos) != 0 {
		t.Fatalf("want 0 relayed streams, got %d", len(infos))
	}
}

func TestListStreamsDedupesRelayedAgainstLister(t *testing.T) {
	lister := func() []StreamInfo {
		return []StreamInfo{{Key: "feedC", Viewers: 5, Protocol: "RTP"}}
	}
	srv := newRelayTestServer(t, lister)
	srv.RegisterRelayedUpstream("feedC", UpstreamConfig{Addr: "a:1"})

	streams := listStreams(t, srv)
	count := 0
	for _, s := range streams {
		if s.Key == "feedC" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("feedC appeared %d times, want 1 (lister entry should win)", count)
	}
	si := findStream(streams, "feedC")
	if si.Viewers != 5 || si.Protocol != "RTP" {
		t.Fatalf("lister entry not preserved; got %+v", *si)
	}
}
