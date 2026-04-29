package demux

import (
	"testing"
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
