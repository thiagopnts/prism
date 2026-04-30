package demux

import (
	"errors"

	"github.com/zsiec/prism/mpegts"
)

// MPEG-4 Audio Object Types referenced by the demuxer. ADTS profile bits
// only express AOT 1..4; HE-AAC (5) and HE-AAC v2 (29) reach the demuxer via
// PMT descriptor (tag 0x1C) signaling.
const (
	aotAACLC   uint8 = 2
	aotHEAAC   uint8 = 5
	aotHEAACv2 uint8 = 29
)

// AACCodecString returns "mp4a.40.<AOT>" for the given Audio Object Type, or
// the empty string for AOTs we don't support.
func AACCodecString(aot uint8) string {
	switch aot {
	case 1:
		return "mp4a.40.1"
	case aotAACLC:
		return "mp4a.40.2"
	case 3:
		return "mp4a.40.3"
	case 4:
		return "mp4a.40.4"
	case aotHEAAC:
		return "mp4a.40.5"
	case aotHEAACv2:
		return "mp4a.40.29"
	}
	return ""
}

// MPEG-4 audio descriptor tag (ISO/IEC 13818-1 §2.6.40) carrying a single
// audioProfileLevelIndication byte that identifies the MPEG-4 Audio profile
// and level for this elementary stream.
const descriptorTagMPEG4Audio = 0x1C

// aotFromAudioProfileLevel maps an MPEG-4 audioProfileLevelIndication value
// (ISO/IEC 14496-3 §1.6.6 Table 1.14) to the Audio Object Type implied by
// that profile/level. Returns 0 for values outside the AAC family.
//
// Profile levels collapse to a single AOT: anything in the HE-AAC family
// becomes AOT 5, anything in the HE-AAC v2 family becomes AOT 29. The level
// distinction is not load-bearing for WebCodecs decoder configuration.
func aotFromAudioProfileLevel(apli uint8) uint8 {
	switch apli {
	case 0x28, 0x29, 0x2A, 0x2B:
		return aotAACLC
	case 0x2C, 0x2D, 0x2E, 0x2F, 0x34, 0x35:
		return aotHEAAC
	case 0x30, 0x31, 0x32, 0x33, 0x36, 0x37:
		return aotHEAACv2
	}
	return 0
}

// audioProfileAOTFromDescriptors scans elementary stream descriptors for the
// MPEG-4 audio descriptor and returns its mapped AOT, or 0 if no such
// descriptor is present. Used to detect HE-AAC streams that explicitly
// signal SBR/PS via PMT (the ADTS profile bits cannot).
func audioProfileAOTFromDescriptors(descs []mpegts.PMTDescriptor) uint8 {
	for _, d := range descs {
		if d.Tag == descriptorTagMPEG4Audio && len(d.Data) >= 1 {
			if aot := aotFromAudioProfileLevel(d.Data[0]); aot != 0 {
				return aot
			}
		}
	}
	return 0
}

// ErrInvalidADTS is returned when the ADTS sync word or header is malformed.
// The parser currently never returns this error; recoverable per-frame issues
// are skipped instead so earlier valid frames are not stranded. Kept exported
// for API stability.
var ErrInvalidADTS = errors.New("invalid ADTS header")

// AAC sample rate index table (ISO/IEC 14496-3 §1.6.3.4 Table 1.16).
var aacSampleRates = [...]int{
	96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050,
	16000, 12000, 11025, 8000, 7350,
}

// AAC channel_configuration → channel count (ISO/IEC 14496-3 Table 1.19).
// Index 0 means "defined elsewhere" (Program Config Element); count is unknown
// from the ADTS header alone, so we report 0.
var aacChannelCounts = [...]int{0, 1, 2, 3, 4, 5, 6, 8}

// AACFrame represents a single AAC raw_data_block parsed from an ADTS stream.
type AACFrame struct {
	Data          []byte // complete ADTS frame (header + payload)
	SampleRate    int    // resolved from sampling_frequency_index
	Channels      int    // actual channel count (0 if channel_configuration == 0)
	ChannelConfig uint8  // raw 3-bit channel_configuration (0–7) — used for MoQ catalog
	AOT           uint8  // MPEG-4 Audio Object Type from ADTS profile+1 (1=MAIN, 2=LC, 3=SSR, 4=LTP)
}

// ParseADTS parses an ADTS byte stream into individual AAC frames.
//
// Recoverable per-frame issues (bad sample-rate index, layer != 0, or a frame
// carrying multiple raw_data_blocks) are skipped without aborting the stream
// so earlier valid frames are never lost. Truly malformed sync words trigger a
// byte-by-byte resync.
//
// CRC validation is intentionally skipped — the optional 16-bit CRC covers
// only the ADTS header (not payload) and the underlying MPEG-TS layer already
// provides framing integrity. TODO: add CRC verification if a use case appears.
func ParseADTS(data []byte) ([]AACFrame, error) {
	var frames []AACFrame
	offset := 0

	for offset < len(data) {
		if len(data)-offset < 7 {
			break // not enough for ADTS header
		}

		// Sync word: 12 bits all set (0xFFF).
		if data[offset] != 0xFF || (data[offset+1]&0xF0) != 0xF0 {
			offset++
			continue
		}

		// Layer must be 0 per spec; non-zero almost always means false sync.
		if (data[offset+1]>>1)&0x03 != 0 {
			offset++
			continue
		}

		// protection_absent: bit 0 of byte 1. 0 means a 16-bit CRC follows.
		hasCRC := (data[offset+1] & 0x01) == 0
		headerSize := 7
		if hasCRC {
			headerSize = 9 // TODO: validate CRC at bytes 7-8
		}

		aot := ((data[offset+2] >> 6) & 0x03) + 1

		sampleRateIdx := (data[offset+2] >> 2) & 0x0F
		if int(sampleRateIdx) >= len(aacSampleRates) {
			// Reserved/escape value — skip this byte and resync.
			offset++
			continue
		}

		channelCfg := ((data[offset+2] & 0x01) << 2) | ((data[offset+3] >> 6) & 0x03)

		frameLen := int(data[offset+3]&0x03)<<11 |
			int(data[offset+4])<<3 |
			int(data[offset+5]>>5)

		if frameLen < headerSize || offset+frameLen > len(data) {
			break // truncated
		}

		// number_of_raw_data_blocks_in_frame: bottom 2 bits of byte 6.
		// Value N means N+1 raw blocks. We only support single-block frames;
		// multi-block frames need raw_data_blocks() distance pointers.
		if data[offset+6]&0x03 != 0 {
			offset += frameLen
			continue
		}

		frames = append(frames, AACFrame{
			Data:          data[offset : offset+frameLen],
			SampleRate:    aacSampleRates[sampleRateIdx],
			Channels:      aacChannelCounts[channelCfg], // channelCfg is 3 bits, table has 8 entries
			ChannelConfig: channelCfg,
			AOT:           aot,
		})

		offset += frameLen
	}

	return frames, nil
}
