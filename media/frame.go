// Package media defines the core frame types that flow through the Prism
// processing pipeline, from demuxing through distribution.
package media

// Channel buffer sizes used by both the demuxer (producer) and viewer sessions
// (consumer) to decouple frame production from consumption. Sized to absorb
// jitter without excessive memory: ~2 seconds of video, ~2.5s of audio.
const (
	VideoBufferSize   = 60
	AudioBufferSize   = 120
	CaptionBufferSize = 30
)

// VideoFrame represents a single decoded video access unit (one picture) ready
// for relay to viewers. It carries the raw NAL units in Annex B format along
// with parameter sets needed by decoders to initialize or reconfigure.
type VideoFrame struct {
	PTS        int64
	DTS        int64
	IsKeyframe bool
	NALUs      [][]byte
	SPS        []byte
	PPS        []byte
	VPS        []byte
	Codec      string // "h264" or "h265"
	GroupID    uint32
	WireData   []byte // pre-serialized AVC1 (length-prefixed) NALUs for distribution
}

// AudioFrame represents a single AAC audio frame (ADTS-wrapped) belonging
// to a specific audio track. Multi-track streams produce separate AudioFrames
// with distinct TrackIndex values.
//
// Channels is the actual channel count derived from the ADTS
// channel_configuration via ISO/IEC 14496-3 Table 1.19 (cfg=7 → 8 channels).
// ChannelConfig is the raw 3-bit configuration value (0–7) preserved for
// downstream consumers like the MoQ catalog. Codec is the canonical MIME-style
// codec id ("mp4a.40.<AOT>") derived from the ADTS profile field.
type AudioFrame struct {
	PTS           int64
	Data          []byte
	SampleRate    int
	Channels      int
	ChannelConfig uint8
	Codec         string
	TrackIndex    int
}
