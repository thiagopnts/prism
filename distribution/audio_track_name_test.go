package distribution

import "testing"

func TestAudioTrackName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		index int
		lang  string
		want  string
	}{
		{0, "", "audio0"},
		{0, "eng", "audio0-eng"},
		{1, "es", "audio1-es"},
		{2, "", "audio2"},
		{12, "ENG", "audio12-ENG"},
	}
	for _, tc := range cases {
		if got := audioTrackName(tc.index, tc.lang); got != tc.want {
			t.Fatalf("audioTrackName(%d, %q) = %q, want %q", tc.index, tc.lang, got, tc.want)
		}
	}
}

func TestParseAudioTrackIndex(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		wantIdx int
		wantOK  bool
	}{
		{"audio0", 0, true},
		{"audio0-eng", 0, true},
		{"audio1-es", 1, true},
		{"audio12", 12, true},
		{"audio12-spa", 12, true},
		{"audio", 0, false},
		{"audiox", 0, false},
		{"audio0eng", 0, false},
		{"audio-eng", 0, false},
		{"video", 0, false},
		{"captions", 0, false},
		{"", 0, false},
	}
	for _, tc := range cases {
		gotIdx, gotOK := parseAudioTrackIndex(tc.name)
		if gotOK != tc.wantOK || (gotOK && gotIdx != tc.wantIdx) {
			t.Fatalf("parseAudioTrackIndex(%q) = (%d, %v), want (%d, %v)",
				tc.name, gotIdx, gotOK, tc.wantIdx, tc.wantOK)
		}
	}
}
