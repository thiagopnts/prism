package distribution

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/zsiec/prism/media"
)

func TestBuildMoQCatalogBasic(t *testing.T) {
	t.Parallel()
	relay := NewRelay()
	// Feed with one audio track: observed from a broadcast audio frame.
	relay.BroadcastAudio(&media.AudioFrame{TrackIndex: 0, SampleRate: 48000, Channels: 2})
	data, err := buildMoQCatalog("teststream", relay, false)
	if err != nil {
		t.Fatal(err)
	}

	var cat moqCatalog
	if err := json.Unmarshal(data, &cat); err != nil {
		t.Fatal(err)
	}

	if cat.Version != 1 {
		t.Fatalf("version = %d, want 1", cat.Version)
	}
	if cat.StreamingFormat != 1 {
		t.Fatalf("streamingFormat = %d, want 1", cat.StreamingFormat)
	}
	if cat.StreamingFormatVersion != "0.2" {
		t.Fatalf("streamingFormatVersion = %q, want 0.2", cat.StreamingFormatVersion)
	}
	if cat.CommonTrackFields.Namespace != "prism/teststream" {
		t.Fatalf("namespace = %q", cat.CommonTrackFields.Namespace)
	}
	if cat.CommonTrackFields.Packaging != "loc" {
		t.Fatalf("packaging = %q", cat.CommonTrackFields.Packaging)
	}

	// One audio track → video + audio0 + captions + stats = 4 tracks
	if len(cat.Tracks) != 4 {
		t.Fatalf("track count = %d, want 4", len(cat.Tracks))
	}

	// Video
	if cat.Tracks[0].Name != "video" {
		t.Fatalf("tracks[0].name = %q", cat.Tracks[0].Name)
	}
	if cat.Tracks[0].SelectionParams.Width != 1920 {
		t.Fatalf("video width = %d", cat.Tracks[0].SelectionParams.Width)
	}

	// Audio
	if cat.Tracks[1].Name != "audio0" {
		t.Fatalf("tracks[1].name = %q", cat.Tracks[1].Name)
	}
	if cat.Tracks[1].SelectionParams.Codec != "mp4a.40.02" {
		t.Fatalf("audio codec = %q", cat.Tracks[1].SelectionParams.Codec)
	}
	if cat.Tracks[1].SelectionParams.SampleRate != 48000 {
		t.Fatalf("audio sampleRate = %d", cat.Tracks[1].SelectionParams.SampleRate)
	}
	if cat.Tracks[1].SelectionParams.ChannelConfig != "2" {
		t.Fatalf("audio channelConfig = %q", cat.Tracks[1].SelectionParams.ChannelConfig)
	}

	// Captions
	if cat.Tracks[2].Name != "captions" {
		t.Fatalf("tracks[2].name = %q", cat.Tracks[2].Name)
	}
	if cat.Tracks[2].SelectionParams.Codec != "caption/v2" {
		t.Fatalf("caption codec = %q", cat.Tracks[2].SelectionParams.Codec)
	}

	// Stats
	if cat.Tracks[3].Name != "stats" {
		t.Fatalf("tracks[3].name = %q", cat.Tracks[3].Name)
	}
	if cat.Tracks[3].SelectionParams.Codec != "application/json" {
		t.Fatalf("stats codec = %q", cat.Tracks[3].SelectionParams.Codec)
	}
}

func TestBuildMoQCatalogNoAudio(t *testing.T) {
	t.Parallel()
	// A video-only feed never sets an audio track count (it stays zero); the
	// catalog must not advertise a phantom audio track.
	relay := NewRelay()

	data, err := buildMoQCatalog("video-only", relay, false)
	if err != nil {
		t.Fatal(err)
	}

	var cat moqCatalog
	if err := json.Unmarshal(data, &cat); err != nil {
		t.Fatal(err)
	}

	// video + captions + stats = 3 tracks, no audio.
	if len(cat.Tracks) != 3 {
		t.Fatalf("track count = %d, want 3", len(cat.Tracks))
	}
	for _, tr := range cat.Tracks {
		if strings.HasPrefix(tr.Name, "audio") {
			t.Fatalf("unexpected audio track %q in video-only catalog", tr.Name)
		}
	}
	if cat.Tracks[0].Name != "video" {
		t.Fatalf("tracks[0].name = %q, want video", cat.Tracks[0].Name)
	}
	if cat.Tracks[1].Name != "captions" {
		t.Fatalf("tracks[1].name = %q, want captions", cat.Tracks[1].Name)
	}
	if cat.Tracks[2].Name != "stats" {
		t.Fatalf("tracks[2].name = %q, want stats", cat.Tracks[2].Name)
	}
}

func TestBuildMoQCatalogMultiAudio(t *testing.T) {
	t.Parallel()
	relay := NewRelay()
	// Three audio tracks observed from broadcast frames.
	for i := 0; i < 3; i++ {
		relay.BroadcastAudio(&media.AudioFrame{TrackIndex: i, SampleRate: 48000, Channels: 2})
	}

	data, err := buildMoQCatalog("multi", relay, false)
	if err != nil {
		t.Fatal(err)
	}

	var cat moqCatalog
	if err := json.Unmarshal(data, &cat); err != nil {
		t.Fatal(err)
	}

	// video + audio0 + audio1 + audio2 + captions + stats = 6 tracks
	if len(cat.Tracks) != 6 {
		t.Fatalf("track count = %d, want 6", len(cat.Tracks))
	}

	for i := 0; i < 3; i++ {
		expected := "audio" + string(rune('0'+i))
		if cat.Tracks[i+1].Name != expected {
			t.Fatalf("tracks[%d].name = %q, want %q", i+1, cat.Tracks[i+1].Name, expected)
		}
	}
}

func TestBuildMoQCatalogCustomVideoInfo(t *testing.T) {
	t.Parallel()
	relay := NewRelay()
	relay.mu.Lock()
	relay.videoInfo = VideoInfo{Codec: "avc1.640028", Width: 3840, Height: 2160}
	relay.videoInfoSet = true
	relay.mu.Unlock()

	data, err := buildMoQCatalog("4k", relay, false)
	if err != nil {
		t.Fatal(err)
	}

	var cat moqCatalog
	if err := json.Unmarshal(data, &cat); err != nil {
		t.Fatal(err)
	}

	vp := cat.Tracks[0].SelectionParams
	if vp.Codec != "avc1.640028" {
		t.Fatalf("video codec = %q", vp.Codec)
	}
	if vp.Width != 3840 || vp.Height != 2160 {
		t.Fatalf("video resolution = %dx%d", vp.Width, vp.Height)
	}
}

func TestBuildMoQCatalogJSONFieldNames(t *testing.T) {
	t.Parallel()
	relay := NewRelay()
	data, err := buildMoQCatalog("test", relay, false)
	if err != nil {
		t.Fatal(err)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}

	// Check that JSON keys match spec exactly
	requiredKeys := []string{"version", "streamingFormat", "streamingFormatVersion", "commonTrackFields", "tracks"}
	for _, key := range requiredKeys {
		if _, ok := raw[key]; !ok {
			t.Fatalf("missing required JSON key: %q", key)
		}
	}

	ctf := raw["commonTrackFields"].(map[string]any)
	if _, ok := ctf["namespace"]; !ok {
		t.Fatal("missing commonTrackFields.namespace")
	}
	if _, ok := ctf["packaging"]; !ok {
		t.Fatal("missing commonTrackFields.packaging")
	}
}

func TestBuildMoQCatalogObservedAudioParams(t *testing.T) {
	t.Parallel()
	relay := NewRelay()
	// Audio params are taken from the observed frame (sample rate / channels);
	// the codec is always AAC-LC, the only codec the demuxer produces.
	relay.BroadcastAudio(&media.AudioFrame{TrackIndex: 0, SampleRate: 44100, Channels: 1})

	data, err := buildMoQCatalog("custom-audio", relay, false)
	if err != nil {
		t.Fatal(err)
	}

	var cat moqCatalog
	if err := json.Unmarshal(data, &cat); err != nil {
		t.Fatal(err)
	}

	ap := cat.Tracks[1].SelectionParams
	if ap.Codec != "mp4a.40.02" {
		t.Fatalf("audio codec = %q", ap.Codec)
	}
	if ap.SampleRate != 44100 {
		t.Fatalf("audio sampleRate = %d", ap.SampleRate)
	}
	if ap.ChannelConfig != "1" {
		t.Fatalf("audio channelConfig = %q", ap.ChannelConfig)
	}
}

func TestBuildMoQCatalogControlTrack(t *testing.T) {
	t.Parallel()
	relay := NewRelay()

	// Video-only default relay. Without control enabled: 3 tracks
	// (video + captions + stats)
	dataNoControl, err := buildMoQCatalog("test", relay, false)
	if err != nil {
		t.Fatal(err)
	}
	var catNoControl moqCatalog
	if err := json.Unmarshal(dataNoControl, &catNoControl); err != nil {
		t.Fatal(err)
	}
	if len(catNoControl.Tracks) != 3 {
		t.Fatalf("without control: track count = %d, want 3", len(catNoControl.Tracks))
	}

	// With control enabled: 4 tracks (video + captions + stats + control)
	dataWithControl, err := buildMoQCatalog("test", relay, true)
	if err != nil {
		t.Fatal(err)
	}
	var catWithControl moqCatalog
	if err := json.Unmarshal(dataWithControl, &catWithControl); err != nil {
		t.Fatal(err)
	}
	if len(catWithControl.Tracks) != 4 {
		t.Fatalf("with control: track count = %d, want 4", len(catWithControl.Tracks))
	}

	// Verify the control track is last and has the right codec
	controlTrack := catWithControl.Tracks[3]
	if controlTrack.Name != "control" {
		t.Fatalf("control track name = %q, want %q", controlTrack.Name, "control")
	}
	if controlTrack.SelectionParams.Codec != "application/json" {
		t.Fatalf("control track codec = %q, want %q", controlTrack.SelectionParams.Codec, "application/json")
	}
}

func TestBuildMoQCatalogAudioLanguage(t *testing.T) {
	t.Parallel()
	relay := NewRelay()
	// Track 0 has a language, track 1 does not, track 2 does — index still
	// progresses regardless of label presence.
	relay.BroadcastAudio(&media.AudioFrame{TrackIndex: 0, SampleRate: 48000, Channels: 2, Language: "eng"})
	relay.BroadcastAudio(&media.AudioFrame{TrackIndex: 1, SampleRate: 48000, Channels: 2})
	relay.BroadcastAudio(&media.AudioFrame{TrackIndex: 2, SampleRate: 48000, Channels: 2, Language: "es"})

	data, err := buildMoQCatalog("langstream", relay, false)
	if err != nil {
		t.Fatal(err)
	}

	var cat moqCatalog
	if err := json.Unmarshal(data, &cat); err != nil {
		t.Fatal(err)
	}

	// video + audio0-eng + audio1 + audio2-es + captions + stats = 6 tracks
	if len(cat.Tracks) != 6 {
		t.Fatalf("track count = %d, want 6", len(cat.Tracks))
	}
	wantNames := []string{"audio0-eng", "audio1", "audio2-es"}
	for i, want := range wantNames {
		if cat.Tracks[i+1].Name != want {
			t.Fatalf("tracks[%d].name = %q, want %q", i+1, cat.Tracks[i+1].Name, want)
		}
	}
}
