package demux

import (
	"context"
	"testing"
	"time"
)

// buildHEVCCaptionSEI builds an HEVC prefix-SEI NAL unit (2-byte NAL header)
// carrying an ATSC A/53 (ITU-T T.35) user_data payload with the given CEA-608
// cc triplets ([marker, cc1, cc2]). The returned bytes include the 2-byte HEVC
// NAL header and no start code, matching NALUnit.Data.
func buildHEVCCaptionSEI(triplets [][3]byte) []byte {
	payload := []byte{
		0xB5,             // itu_t_t35_country_code (USA)
		0x00, 0x31,       // itu_t_t35_provider_code (ATSC)
		'G', 'A', '9', '4', // user_identifier
		0x03, // user_data_type_code (cc_data)
	}
	payload = append(payload, 0xC0|byte(len(triplets))) // process_cc_data_flag + cc_count
	payload = append(payload, 0xFF)                     // em_data
	for _, tr := range triplets {
		payload = append(payload, tr[0], tr[1], tr[2])
	}

	sei := []byte{0x04, byte(len(payload))} // payloadType 4, payloadSize
	sei = append(sei, payload...)

	nal := []byte{0x4E, 0x01} // HEVC NAL header: type 39 (PREFIX_SEI), temporal_id_plus1=1
	return append(nal, sei...)
}

// TestDemuxer_HEVCCaptionExtraction verifies that CEA-608 captions carried in
// an HEVC SEI NAL are decoded and emitted. HEVC SEI NALs use a 2-byte header,
// so the demuxer must use the HEVC-aware extractor; using the H.264 extractor
// misaligns the SEI payload by one byte and drops every caption.
func TestDemuxer_HEVCCaptionExtraction(t *testing.T) {
	d := NewDemuxer(nil, nil)
	d.isHEVC = true

	// CEA-608 field 1 (cc_type 0): RU2 (roll-up 2) then the characters "Hi".
	nal := buildHEVCCaptionSEI([][3]byte{
		{0xFC, 0x14, 0x25}, // valid, cc_type 0: RU2 control
		{0xFC, 0x48, 0x69}, // valid, cc_type 0: 'H', 'i'
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go d.handleCaptionSEI(ctx, nal, 12345)

	select {
	case frame := <-d.Captions():
		if frame == nil {
			t.Fatal("received nil caption frame")
		}
		if frame.Text != "Hi" {
			t.Errorf("caption text: got %q, want %q", frame.Text, "Hi")
		}
		if frame.Channel != 1 {
			t.Errorf("caption channel: got %d, want 1", frame.Channel)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for HEVC caption frame: no caption decoded")
	}
}
