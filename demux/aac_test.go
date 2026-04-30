package demux

import (
	"testing"

	"github.com/zsiec/prism/mpegts"
)

// buildADTSHeader writes a 7-byte ADTS header (no CRC) into hdr describing a
// frame of the given total byte length. profile/sampleRateIdx/channelCfg are
// the raw header fields. nrdb is number_of_raw_data_blocks_in_frame.
func buildADTSHeader(hdr []byte, frameLen int, profile, sampleRateIdx, channelCfg, nrdb byte) {
	hdr[0] = 0xFF
	hdr[1] = 0xF1 // sync(4) | ID=0 | layer=0 | protection_absent=1
	hdr[2] = (profile << 6) | (sampleRateIdx << 2) | ((channelCfg >> 2) & 0x01)
	hdr[3] = ((channelCfg & 0x03) << 6) | byte((frameLen>>11)&0x03)
	hdr[4] = byte((frameLen >> 3) & 0xFF)
	hdr[5] = byte((frameLen&0x07)<<5) | 0x1F // buffer fullness top 5 bits
	hdr[6] = 0xFC | (nrdb & 0x03)            // buffer fullness low 6 + nrdb
}

func TestParseADTS(t *testing.T) {
	t.Parallel()
	// Profile=1 (AAC-LC), sampleRateIdx=3 (48kHz), channelCfg=2 (stereo), nrdb=0.
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE}
	frameLen := 7 + len(payload)

	header := make([]byte, 7)
	buildADTSHeader(header, frameLen, 1, 3, 2, 0)
	adts := append(header, payload...)

	frames, err := ParseADTS(adts)
	if err != nil {
		t.Fatalf("ParseADTS failed: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}

	f := frames[0]
	if f.SampleRate != 48000 {
		t.Errorf("SampleRate = %d, want 48000", f.SampleRate)
	}
	if f.Channels != 2 {
		t.Errorf("Channels = %d, want 2", f.Channels)
	}
	if f.ChannelConfig != 2 {
		t.Errorf("ChannelConfig = %d, want 2", f.ChannelConfig)
	}
	if f.AOT != 2 {
		t.Errorf("AOT = %d, want 2 (LC)", f.AOT)
	}
	if len(f.Data) != frameLen {
		t.Errorf("Data length = %d, want %d", len(f.Data), frameLen)
	}
}

func TestParseADTSEmpty(t *testing.T) {
	t.Parallel()
	frames, err := ParseADTS(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 0 {
		t.Errorf("expected 0 frames for empty input, got %d", len(frames))
	}
}

func TestParseADTSTruncated(t *testing.T) {
	t.Parallel()
	data := []byte{0xFF, 0xF1, 0x50, 0x80, 0x00}
	frames, err := ParseADTS(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 0 {
		t.Errorf("expected 0 frames for truncated input, got %d", len(frames))
	}
}

func TestParseADTSChannelCount7(t *testing.T) {
	t.Parallel()
	// channelCfg=7 must resolve to 8 channels per ISO 14496-3 Table 1.19.
	payload := make([]byte, 4)
	frameLen := 7 + len(payload)
	header := make([]byte, 7)
	buildADTSHeader(header, frameLen, 1, 3, 7, 0)
	adts := append(header, payload...)

	frames, err := ParseADTS(adts)
	if err != nil || len(frames) != 1 {
		t.Fatalf("ParseADTS: err=%v, frames=%d", err, len(frames))
	}
	if frames[0].ChannelConfig != 7 {
		t.Errorf("ChannelConfig = %d, want 7", frames[0].ChannelConfig)
	}
	if frames[0].Channels != 8 {
		t.Errorf("Channels = %d, want 8", frames[0].Channels)
	}
}

func TestParseADTSChannelCount0(t *testing.T) {
	t.Parallel()
	// channelCfg=0 means "defined elsewhere"; count is unknown (0).
	payload := make([]byte, 4)
	frameLen := 7 + len(payload)
	header := make([]byte, 7)
	buildADTSHeader(header, frameLen, 1, 3, 0, 0)
	adts := append(header, payload...)

	frames, err := ParseADTS(adts)
	if err != nil || len(frames) != 1 {
		t.Fatalf("ParseADTS: err=%v, frames=%d", err, len(frames))
	}
	if frames[0].ChannelConfig != 0 {
		t.Errorf("ChannelConfig = %d, want 0", frames[0].ChannelConfig)
	}
	if frames[0].Channels != 0 {
		t.Errorf("Channels = %d, want 0", frames[0].Channels)
	}
}

func TestParseADTSProfileAOT(t *testing.T) {
	t.Parallel()
	cases := []struct {
		profile byte
		wantAOT uint8
	}{
		{0, 1}, // MAIN
		{1, 2}, // LC
		{2, 3}, // SSR
		{3, 4}, // LTP
	}
	for _, tc := range cases {
		payload := make([]byte, 4)
		frameLen := 7 + len(payload)
		header := make([]byte, 7)
		buildADTSHeader(header, frameLen, tc.profile, 3, 2, 0)
		adts := append(header, payload...)

		frames, err := ParseADTS(adts)
		if err != nil || len(frames) != 1 {
			t.Fatalf("profile=%d: err=%v, frames=%d", tc.profile, err, len(frames))
		}
		if frames[0].AOT != tc.wantAOT {
			t.Errorf("profile=%d: AOT = %d, want %d", tc.profile, frames[0].AOT, tc.wantAOT)
		}
	}
}

func TestParseADTSLayerNonZero(t *testing.T) {
	t.Parallel()
	// Layer must be 0; a frame with layer != 0 must be treated as false sync.
	payload := make([]byte, 4)
	frameLen := 7 + len(payload)
	header := make([]byte, 7)
	buildADTSHeader(header, frameLen, 1, 3, 2, 0)
	header[1] = 0xF3 // sync(4) | ID=0 | layer=01 (illegal) | protection_absent=1
	adts := append(header, payload...)

	frames, err := ParseADTS(adts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 0 {
		t.Errorf("expected 0 frames for layer != 0, got %d", len(frames))
	}
}

func TestParseADTSMultiBlock(t *testing.T) {
	t.Parallel()
	// number_of_raw_data_blocks_in_frame=1 → currently unsupported, frame skipped.
	payload := make([]byte, 4)
	frameLen := 7 + len(payload)
	header := make([]byte, 7)
	buildADTSHeader(header, frameLen, 1, 3, 2, 1)
	adts := append(header, payload...)

	frames, err := ParseADTS(adts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 0 {
		t.Errorf("expected 0 frames for multi-block ADTS, got %d", len(frames))
	}
}

func TestParseADTSBadSampleRateRecoverable(t *testing.T) {
	t.Parallel()
	// First frame has reserved sampleRateIdx=13; should be skipped without
	// stranding subsequent valid frames.
	bad := make([]byte, 11)
	buildADTSHeader(bad, 11, 1, 13, 2, 0)

	good := make([]byte, 11)
	buildADTSHeader(good, 11, 1, 3, 2, 0)

	adts := append(bad, good...)
	frames, err := ParseADTS(adts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if frames[0].SampleRate != 48000 {
		t.Errorf("SampleRate = %d, want 48000", frames[0].SampleRate)
	}
}

func TestAACCodecString(t *testing.T) {
	t.Parallel()
	cases := []struct {
		aot  uint8
		want string
	}{
		{1, "mp4a.40.1"},
		{2, "mp4a.40.2"},
		{3, "mp4a.40.3"},
		{4, "mp4a.40.4"},
		{5, "mp4a.40.5"},
		{29, "mp4a.40.29"},
		{0, ""},
		{6, ""},
		{30, ""},
	}
	for _, tc := range cases {
		if got := AACCodecString(tc.aot); got != tc.want {
			t.Errorf("AACCodecString(%d) = %q, want %q", tc.aot, got, tc.want)
		}
	}
}

func TestAOTFromAudioProfileLevel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		apli uint8
		want uint8
		name string
	}{
		{0x28, 2, "AAC L1"},
		{0x29, 2, "AAC L2"},
		{0x2A, 2, "AAC L4"},
		{0x2B, 2, "AAC L5"},
		{0x2C, 5, "HE-AAC L2"},
		{0x2D, 5, "HE-AAC L3"},
		{0x2E, 5, "HE-AAC L4"},
		{0x2F, 5, "HE-AAC L5"},
		{0x34, 5, "HE-AAC L6"},
		{0x35, 5, "HE-AAC L7"},
		{0x30, 29, "HE-AAC v2 L2"},
		{0x31, 29, "HE-AAC v2 L3"},
		{0x32, 29, "HE-AAC v2 L4"},
		{0x33, 29, "HE-AAC v2 L5"},
		{0x36, 29, "HE-AAC v2 L6"},
		{0x37, 29, "HE-AAC v2 L7"},
		{0x00, 0, "reserved 0"},
		{0x27, 0, "below AAC range"},
		{0x38, 0, "above HE-AAC v2 range"},
		{0xFF, 0, "non-AAC profile"},
	}
	for _, tc := range cases {
		if got := aotFromAudioProfileLevel(tc.apli); got != tc.want {
			t.Errorf("%s: aotFromAudioProfileLevel(0x%02X) = %d, want %d",
				tc.name, tc.apli, got, tc.want)
		}
	}
}

func TestAudioProfileAOTFromDescriptors(t *testing.T) {
	t.Parallel()

	t.Run("HE-AAC descriptor present", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: descriptorTagMPEG4Audio, Data: []byte{0x2D}}, // HE-AAC L3
		}
		if got := audioProfileAOTFromDescriptors(descs); got != 5 {
			t.Errorf("got AOT=%d, want 5", got)
		}
	})

	t.Run("HE-AAC v2 descriptor present", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: descriptorTagMPEG4Audio, Data: []byte{0x31}}, // HE-AAC v2 L3
		}
		if got := audioProfileAOTFromDescriptors(descs); got != 29 {
			t.Errorf("got AOT=%d, want 29", got)
		}
	})

	t.Run("LC profile is reported as AOT 2", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: descriptorTagMPEG4Audio, Data: []byte{0x29}}, // AAC L2
		}
		if got := audioProfileAOTFromDescriptors(descs); got != 2 {
			t.Errorf("got AOT=%d, want 2", got)
		}
	})

	t.Run("no descriptor", func(t *testing.T) {
		if got := audioProfileAOTFromDescriptors(nil); got != 0 {
			t.Errorf("got AOT=%d, want 0", got)
		}
	})

	t.Run("unrelated descriptor only", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x05, Data: []byte{0x01, 0x02}}, // registration descriptor
		}
		if got := audioProfileAOTFromDescriptors(descs); got != 0 {
			t.Errorf("got AOT=%d, want 0", got)
		}
	})

	t.Run("MPEG-4 audio descriptor with empty data", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: descriptorTagMPEG4Audio, Data: nil},
		}
		if got := audioProfileAOTFromDescriptors(descs); got != 0 {
			t.Errorf("got AOT=%d, want 0", got)
		}
	})

	t.Run("MPEG-4 audio descriptor among others picks AAC family", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x05, Data: []byte{'A', 'C', '-', '3'}},
			{Tag: descriptorTagMPEG4Audio, Data: []byte{0x2C}}, // HE-AAC L2
			{Tag: 0x52, Data: []byte{0x01}},
		}
		if got := audioProfileAOTFromDescriptors(descs); got != 5 {
			t.Errorf("got AOT=%d, want 5", got)
		}
	})

	t.Run("descriptor with non-AAC profile value", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: descriptorTagMPEG4Audio, Data: []byte{0xFF}},
		}
		if got := audioProfileAOTFromDescriptors(descs); got != 0 {
			t.Errorf("got AOT=%d, want 0", got)
		}
	})
}

func TestParseADTSCRCProtected(t *testing.T) {
	t.Parallel()
	// protection_absent=0 → 9-byte header. CRC bytes are not validated, but
	// the frame must parse and Data must include the full frame including CRC.
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	frameLen := 9 + len(payload)
	header := make([]byte, 9)
	buildADTSHeader(header[:7], frameLen, 1, 3, 2, 0)
	header[1] = 0xF0 // clear protection_absent bit (CRC present)
	// CRC bytes 7-8 left as zero; parser doesn't validate.

	adts := append(header, payload...)
	frames, err := ParseADTS(adts)
	if err != nil || len(frames) != 1 {
		t.Fatalf("ParseADTS: err=%v, frames=%d", err, len(frames))
	}
	if len(frames[0].Data) != frameLen {
		t.Errorf("Data length = %d, want %d", len(frames[0].Data), frameLen)
	}
}
