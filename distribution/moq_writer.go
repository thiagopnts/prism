package distribution

import (
	"io"

	"github.com/quic-go/quic-go/quicvarint"
	"github.com/zsiec/prism/media"
	"github.com/zsiec/prism/moq"
)

// Compile-time interface check.
var _ StreamFrameWriter = (*moqWriter)(nil)

// MoQ stream type constants (draft-ietf-moq-transport-15).
const (
	// moqStreamTypeSubgroupSIDExt indicates a subgroup stream with an explicit
	// Subgroup ID in the header and per-object extension headers.
	moqStreamTypeSubgroupSIDExt uint64 = 0x0d
)

// LOC header extension IDs (draft-ietf-moq-loc-01).
const (
	locExtCaptureTimestamp  uint64 = 2  // even: varint value = microseconds (media clock)
	locExtVideoFrameMarking uint64 = 4  // even: varint value = RFC 9626 flags
	locExtCaptureWallClock  uint64 = 8  // even: varint value = UTC µs from embedded timecode (0/absent = none)
	locExtVideoConfig       uint64 = 13 // odd: length-prefixed byte string
)

// RFC 9626 Video Frame Marking flags (non-scalable).
const (
	vfmKeyframe    uint64 = 0xE0 // S=1, E=1, I=1 (independent/keyframe)
	vfmNonKeyframe uint64 = 0xC0 // S=1, E=1, I=0 (dependent/delta)
)

// moqWriter implements StreamFrameWriter using MoQ Transport data stream
// framing with LOC header extensions. It produces:
//   - Subgroup headers with QUIC varint fields
//   - Object headers with LOC extensions (capture timestamp, video frame marking, video config)
//   - AVC1 video payloads (length-prefixed NALUs)
//   - Raw AAC audio payloads (ADTS headers stripped)
type moqWriter struct {
	trackAlias        uint64
	publisherPriority byte
	objectID          uint64
}

// NewMoQWriter returns a StreamFrameWriter that produces MoQ-compliant data
// stream framing. trackAlias is a session-scoped identifier for the track,
// and publisherPriority sets the priority (0=highest, 255=lowest).
func NewMoQWriter(trackAlias uint64, publisherPriority byte) StreamFrameWriter {
	return &moqWriter{
		trackAlias:        trackAlias,
		publisherPriority: publisherPriority,
	}
}

func (m *moqWriter) WriteStreamHeader(w io.Writer, _ byte, groupID uint32, _ uint32) error {
	m.objectID = 0

	var buf []byte
	buf = quicvarint.Append(buf, moqStreamTypeSubgroupSIDExt)
	buf = quicvarint.Append(buf, m.trackAlias)
	buf = quicvarint.Append(buf, uint64(groupID))
	buf = quicvarint.Append(buf, 0) // subgroup ID
	buf = append(buf, m.publisherPriority)

	_, err := w.Write(buf)
	return err
}

func (m *moqWriter) WriteVideoFrame(w io.Writer, frame *media.VideoFrame) (int64, error) {
	payload := frame.WireData
	if payload == nil {
		payload = moq.AnnexBToAVC1(frame.NALUs)
	}

	var exts []byte

	// Capture Timestamp (ID 2, even → varint value)
	exts = quicvarint.Append(exts, locExtCaptureTimestamp)
	exts = quicvarint.Append(exts, uint64(frame.PTS))

	// Video Frame Marking (ID 4, even → varint value)
	exts = quicvarint.Append(exts, locExtVideoFrameMarking)
	if frame.IsKeyframe {
		exts = quicvarint.Append(exts, vfmKeyframe)
	} else {
		exts = quicvarint.Append(exts, vfmNonKeyframe)
	}

	// Capture Wall Clock (ID 8, even → varint value): the frame's real capture time
	// in UTC microseconds, reconstructed from an embedded SMPTE timecode. Emitted
	// only when known so streams without a timecode stay byte-identical to before.
	if frame.CaptureWallUS > 0 {
		exts = quicvarint.Append(exts, locExtCaptureWallClock)
		exts = quicvarint.Append(exts, uint64(frame.CaptureWallUS))
	}

	// Video Config on keyframes (ID 13, odd → length-prefixed bytes)
	if frame.IsKeyframe && frame.SPS != nil && frame.PPS != nil {
		var configData []byte
		if frame.Codec == "h265" && frame.VPS != nil {
			configData = moq.BuildHEVCDecoderConfig(frame.VPS, frame.SPS, frame.PPS)
		} else {
			configData = moq.BuildAVCDecoderConfig(frame.SPS, frame.PPS)
		}
		if configData != nil {
			exts = quicvarint.Append(exts, locExtVideoConfig)
			exts = quicvarint.Append(exts, uint64(len(configData)))
			exts = append(exts, configData...)
		}
	}

	return m.writeObject(w, exts, payload)
}

func (m *moqWriter) WriteAudioFrame(w io.Writer, data []byte, timestampMS uint32) (int64, error) {
	payload := moq.StripADTS(data)

	var exts []byte
	exts = quicvarint.Append(exts, locExtCaptureTimestamp)
	exts = quicvarint.Append(exts, uint64(timestampMS)*1000)

	return m.writeObject(w, exts, payload)
}

func (m *moqWriter) WriteDataObject(w io.Writer, data []byte, timestampMS uint32) (int64, error) {
	var exts []byte
	exts = quicvarint.Append(exts, locExtCaptureTimestamp)
	exts = quicvarint.Append(exts, uint64(timestampMS)*1000)

	return m.writeObject(w, exts, data)
}

func (m *moqWriter) StreamHeaderSize() int64 {
	size := quicvarint.Len(moqStreamTypeSubgroupSIDExt) +
		quicvarint.Len(m.trackAlias) +
		1 + // groupID 0 (typical, 1-byte varint)
		1 + // subgroupID 0 (1-byte varint)
		1 // publisher priority
	return int64(size)
}

// RawStreamWriter writes MoQ subgroup framing from caller-supplied field values,
// forwarding the extension block verbatim. Unlike moqWriter it does NOT renumber
// object IDs or synthesize capture-timestamp / frame-marking extensions: a relay
// tier re-serves an upstream publisher's objects byte-for-byte (group, subgroup
// and object IDs, the extension block, and the payload all preserved), rewriting
// only the session-scoped track alias to the downstream viewer's negotiated
// value. One RawStreamWriter is used per downstream uni-stream (subgroup); the
// caller writes the header once, then one or more objects.
type RawStreamWriter struct {
	trackAlias uint64
}

// NewRawStreamWriter returns a writer that emits subgroup framing for the given
// session-scoped downstream track alias.
func NewRawStreamWriter(trackAlias uint64) *RawStreamWriter {
	return &RawStreamWriter{trackAlias: trackAlias}
}

// WriteRawStreamHeader writes the subgroup stream header. Call once per
// uni-stream, before any WriteRawObject. groupID, subgroupID and priority are
// taken verbatim from the upstream subgroup; only the track alias is the
// downstream session's.
func (w *RawStreamWriter) WriteRawStreamHeader(out io.Writer, groupID, subgroupID uint64, priority byte) error {
	var buf []byte
	buf = quicvarint.Append(buf, moqStreamTypeSubgroupSIDExt)
	buf = quicvarint.Append(buf, w.trackAlias)
	buf = quicvarint.Append(buf, groupID)
	buf = quicvarint.Append(buf, subgroupID)
	buf = append(buf, priority)

	_, err := out.Write(buf)
	return err
}

// WriteRawObject writes a single object: the caller-supplied object ID, the
// verbatim extension block (length-prefixed), then the payload (length-prefixed).
// extBytes is forwarded unchanged — pass nil/empty for an object with no
// extensions. Returns the number of bytes written.
func (w *RawStreamWriter) WriteRawObject(out io.Writer, objectID uint64, extBytes, payload []byte) (int64, error) {
	var hdr []byte
	hdr = quicvarint.Append(hdr, objectID)
	hdr = quicvarint.Append(hdr, uint64(len(extBytes)))
	hdr = append(hdr, extBytes...)
	hdr = quicvarint.Append(hdr, uint64(len(payload)))

	total := int64(len(hdr) + len(payload))
	if _, err := out.Write(hdr); err != nil {
		return 0, err
	}
	if _, err := out.Write(payload); err != nil {
		return 0, err
	}
	return total, nil
}

// writeObject writes a MoQ object header (with extensions) and payload.
func (m *moqWriter) writeObject(w io.Writer, exts []byte, payload []byte) (int64, error) {
	var hdr []byte
	hdr = quicvarint.Append(hdr, m.objectID)
	hdr = quicvarint.Append(hdr, uint64(len(exts)))
	hdr = append(hdr, exts...)
	hdr = quicvarint.Append(hdr, uint64(len(payload)))

	m.objectID++

	total := int64(len(hdr) + len(payload))
	if _, err := w.Write(hdr); err != nil {
		return 0, err
	}
	if _, err := w.Write(payload); err != nil {
		return 0, err
	}
	return total, nil
}
