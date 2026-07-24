package demux

import (
	"context"
	"strings"
	"testing"

	"github.com/zsiec/prism/mpegts"
)

func TestSanitizeLanguage(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		raw  []byte
		want string
	}{
		{"lowercase", []byte{'e', 'n', 'g'}, "eng"},
		{"uppercase_preserved", []byte{'E', 'N', 'G'}, "ENG"},
		{"mixed_case_preserved", []byte{'E', 'n', 'g'}, "Eng"},
		{"nul_padded", []byte{'e', 'n', 0x00}, "en"},
		{"space_padded", []byte{'e', 'n', ' '}, "en"},
		{"all_spaces", []byte{' ', ' ', ' '}, ""},
		{"all_nul", []byte{0x00, 0x00, 0x00}, ""},
		{"empty", []byte{}, ""},
		{"digit_rejected", []byte{'e', '1', 'g'}, ""},
		{"interior_space_rejected", []byte{'e', ' ', 'g'}, ""},
		{"non_ascii_rejected", []byte{0xC3, 0xA9, 'g'}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sanitizeLanguage(tc.raw); got != tc.want {
				t.Fatalf("sanitizeLanguage(%v) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestAudioLanguageFromDescriptors(t *testing.T) {
	t.Parallel()

	t.Run("iso639_present", func(t *testing.T) {
		// tag 0x0A, len 4: "eng" + audio_type 0x00
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x0A, Data: []byte{'e', 'n', 'g', 0x00}},
		}
		if got := audioLanguageFromDescriptors(descs); got != "eng" {
			t.Fatalf("got %q, want eng", got)
		}
	})

	t.Run("first_entry_wins", func(t *testing.T) {
		// Two language entries in one descriptor; take the first 3 bytes.
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x0A, Data: []byte{'e', 'n', 'g', 0x00, 's', 'p', 'a', 0x00}},
		}
		if got := audioLanguageFromDescriptors(descs); got != "eng" {
			t.Fatalf("got %q, want eng", got)
		}
	})

	t.Run("skips_other_tags", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x56, Data: []byte{'e', 'n', 'g', 0x11, 0x08}}, // teletext, ignored
			{Tag: 0x0A, Data: []byte{'s', 'p', 'a', 0x00}},
		}
		if got := audioLanguageFromDescriptors(descs); got != "spa" {
			t.Fatalf("got %q, want spa", got)
		}
	})

	t.Run("absent", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x56, Data: []byte{'e', 'n', 'g', 0x11, 0x08}},
		}
		if got := audioLanguageFromDescriptors(descs); got != "" {
			t.Fatalf("got %q, want empty", got)
		}
	})

	t.Run("too_short_ignored", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x0A, Data: []byte{'e', 'n'}}, // < 3 bytes
		}
		if got := audioLanguageFromDescriptors(descs); got != "" {
			t.Fatalf("got %q, want empty", got)
		}
	})

	t.Run("nil", func(t *testing.T) {
		if got := audioLanguageFromDescriptors(nil); got != "" {
			t.Fatalf("got %q, want empty", got)
		}
	})
}

func TestHandleAudioStampsLanguage(t *testing.T) {
	t.Parallel()

	// A minimal valid ADTS frame (7-byte header + 6-byte payload), AAC-LC,
	// 48 kHz, stereo — mirrors demux/aac_test.go construction.
	buildADTS := func() []byte {
		payload := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE}
		frameLen := 7 + len(payload)
		h := make([]byte, 7)
		h[0] = 0xFF
		h[1] = 0xF1                                 // MPEG-4, Layer 0, no CRC
		h[2] = (1 << 6) | (3 << 2)                  // profile=AAC-LC, sr_idx=3(48k)
		h[3] = (2 << 6) | byte((frameLen>>11)&0x03) // channel cfg=2 + frame_len hi
		h[4] = byte((frameLen >> 3) & 0xFF)         // frame_len mid
		h[5] = byte((frameLen&0x07)<<5) | 0x1F      // frame_len lo + buffer fullness
		h[6] = 0xFC                                 // buffer fullness + num_frames-1=0
		return append(h, payload...)
	}

	t.Run("language_present", func(t *testing.T) {
		d := NewDemuxer(strings.NewReader(""), nil)
		d.audioTracks = []AudioTrackInfo{{PID: 494, TrackIndex: 0, Language: "eng"}}

		d.handleAudio(context.Background(), &mpegts.PESData{Data: buildADTS()}, 0)

		select {
		case frame := <-d.Audio():
			if frame.TrackIndex != 0 {
				t.Fatalf("TrackIndex = %d, want 0", frame.TrackIndex)
			}
			if frame.Language != "eng" {
				t.Fatalf("Language = %q, want eng", frame.Language)
			}
		default:
			t.Fatal("no audio frame emitted")
		}
	})

	t.Run("no_language", func(t *testing.T) {
		d := NewDemuxer(strings.NewReader(""), nil)
		d.audioTracks = []AudioTrackInfo{{PID: 494, TrackIndex: 0, Language: ""}}

		d.handleAudio(context.Background(), &mpegts.PESData{Data: buildADTS()}, 0)

		select {
		case frame := <-d.Audio():
			if frame.Language != "" {
				t.Fatalf("Language = %q, want empty", frame.Language)
			}
		default:
			t.Fatal("no audio frame emitted")
		}
	})

	t.Run("index_out_of_range_is_safe", func(t *testing.T) {
		d := NewDemuxer(strings.NewReader(""), nil)
		// audioTracks empty; handleAudio must not panic and must leave lang empty.
		d.handleAudio(context.Background(), &mpegts.PESData{Data: buildADTS()}, 0)

		select {
		case frame := <-d.Audio():
			if frame.Language != "" {
				t.Fatalf("Language = %q, want empty", frame.Language)
			}
		default:
			t.Fatal("no audio frame emitted")
		}
	})
}
