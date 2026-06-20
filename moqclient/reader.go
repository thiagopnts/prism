package moqclient

import (
	"bytes"
	"fmt"
	"io"

	"github.com/quic-go/quic-go/quicvarint"
)

// Stream type for subgroup with stream ID extension (MoQ draft-15).
const StreamTypeSubgroupSIDExt uint64 = 0x0d

// LOC header extension IDs (draft-ietf-moq-loc-01).
const (
	ExtCaptureTimestamp  uint64 = 2  // even: varint microseconds
	ExtVideoFrameMarking uint64 = 4  // even: varint RFC 9626 flags
	ExtVideoConfig       uint64 = 13 // odd: length-prefixed bytes
)

// VFM flag values (RFC 9626 §4).
const (
	VFMKeyframe uint64 = 0xE0 // S=1 E=1 I=1
)

// SubgroupHeader is the stream-level header written once per unidirectional stream.
type SubgroupHeader struct {
	TrackAlias uint64
	GroupID    uint64
	SubgroupID uint64
	Priority   byte
}

// Extension is a single LOC header extension.
type Extension struct {
	ID    uint64
	Value uint64 // valid when ID is even
	Data  []byte // valid when ID is odd; references the sub-buffer slice
}

// Object is a single MoQ object within a subgroup stream.
type Object struct {
	ObjectID   uint64
	Extensions []Extension
	// RawExtBytes is the verbatim extension block as it appeared on the wire
	// (the bytes between the extension-length and payload-length varints), or
	// nil when the object carried no extensions. A relay tier re-serves these
	// bytes unchanged so unknown / odd extensions survive the round trip; the
	// parsed Extensions slice is for inspection only.
	RawExtBytes []byte
	Payload     []byte
}

// ReadSubgroupHeader reads the stream-level header from r.
// Must be called once per stream before any ReadObject calls.
func ReadSubgroupHeader(r io.ByteReader) (SubgroupHeader, error) {
	streamType, err := quicvarint.Read(r)
	if err != nil {
		return SubgroupHeader{}, fmt.Errorf("read stream type: %w", err)
	}
	if streamType != StreamTypeSubgroupSIDExt {
		return SubgroupHeader{}, fmt.Errorf("unexpected stream type 0x%x (want 0x%x)", streamType, StreamTypeSubgroupSIDExt)
	}

	trackAlias, err := quicvarint.Read(r)
	if err != nil {
		return SubgroupHeader{}, fmt.Errorf("read track alias: %w", err)
	}
	groupID, err := quicvarint.Read(r)
	if err != nil {
		return SubgroupHeader{}, fmt.Errorf("read group id: %w", err)
	}
	subgroupID, err := quicvarint.Read(r)
	if err != nil {
		return SubgroupHeader{}, fmt.Errorf("read subgroup id: %w", err)
	}

	// publisher_priority is a single raw byte, not a varint
	priority, err := r.ReadByte()
	if err != nil {
		return SubgroupHeader{}, fmt.Errorf("read priority: %w", err)
	}

	return SubgroupHeader{
		TrackAlias: trackAlias,
		GroupID:    groupID,
		SubgroupID: subgroupID,
		Priority:   priority,
	}, nil
}

// ReadObject reads the next object from a subgroup stream.
// Returns io.EOF when the stream is cleanly closed.
func ReadObject(r io.ByteReader) (Object, error) {
	objectID, err := quicvarint.Read(r)
	if err != nil {
		return Object{}, err // preserve io.EOF
	}

	extByteLen, err := quicvarint.Read(r)
	if err != nil {
		return Object{}, fmt.Errorf("read ext byte length: %w", err)
	}

	// Read the extensions block into a sub-buffer so we consume exactly
	// extByteLen bytes. The buffer is retained verbatim on the Object
	// (RawExtBytes) so a relay can forward the block unchanged, and also parsed
	// into the Extensions slice for inspection.
	var (
		exts   []Extension
		extBuf []byte
	)
	if extByteLen > 0 {
		extBuf = make([]byte, extByteLen)
		if _, err := io.ReadFull(byteReaderToReader(r), extBuf); err != nil {
			return Object{}, fmt.Errorf("read extensions: %w", err)
		}
		exts, err = parseExtensions(extBuf)
		if err != nil {
			return Object{}, fmt.Errorf("parse extensions: %w", err)
		}
	}

	payloadLen, err := quicvarint.Read(r)
	if err != nil {
		return Object{}, fmt.Errorf("read payload length: %w", err)
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(byteReaderToReader(r), payload); err != nil {
			return Object{}, fmt.Errorf("read payload: %w", err)
		}
	}

	return Object{
		ObjectID:    objectID,
		Extensions:  exts,
		RawExtBytes: extBuf,
		Payload:     payload,
	}, nil
}

// parseExtensions parses the extension sub-buffer.
// Unknown extension IDs are silently skipped for forward-compatibility.
func parseExtensions(buf []byte) ([]Extension, error) {
	r := bytes.NewReader(buf)
	var exts []Extension
	for r.Len() > 0 {
		id, err := quicvarint.Read(r)
		if err != nil {
			return nil, fmt.Errorf("read ext id: %w", err)
		}
		if id%2 == 0 {
			// Even: varint value
			val, err := quicvarint.Read(r)
			if err != nil {
				return nil, fmt.Errorf("read ext value (id=%d): %w", id, err)
			}
			exts = append(exts, Extension{ID: id, Value: val})
		} else {
			// Odd: length-prefixed bytes
			dataLen, err := quicvarint.Read(r)
			if err != nil {
				return nil, fmt.Errorf("read ext data len (id=%d): %w", id, err)
			}
			if uint64(r.Len()) < dataLen {
				return nil, fmt.Errorf("ext data truncated (id=%d): need %d bytes, have %d", id, dataLen, r.Len())
			}
			data := make([]byte, dataLen)
			if _, err := io.ReadFull(r, data); err != nil {
				return nil, fmt.Errorf("read ext data (id=%d): %w", id, err)
			}
			exts = append(exts, Extension{ID: id, Data: data})
		}
	}
	return exts, nil
}

// CaptureTimestampUS returns the capture timestamp in microseconds from
// extension ID 2, or 0 if not present.
func CaptureTimestampUS(obj Object) uint64 {
	for _, e := range obj.Extensions {
		if e.ID == ExtCaptureTimestamp {
			return e.Value
		}
	}
	return 0
}

// IsKeyframe returns true if the video frame marking extension (ID 4) has
// the keyframe flag set (0xE0).
func IsKeyframe(obj Object) bool {
	for _, e := range obj.Extensions {
		if e.ID == ExtVideoFrameMarking {
			return e.Value == VFMKeyframe
		}
	}
	return false
}

// VideoConfigBytes returns the raw AVCDecoderConfigRecord or
// HEVCDecoderConfigRecord bytes from extension ID 13, or nil if absent.
func VideoConfigBytes(obj Object) []byte {
	for _, e := range obj.Extensions {
		if e.ID == ExtVideoConfig {
			return e.Data
		}
	}
	return nil
}

// byteReaderToReader returns an io.Reader from an io.ByteReader.
// bytes.Reader already implements io.Reader so the fast path avoids wrapping.
func byteReaderToReader(r io.ByteReader) io.Reader {
	if rd, ok := r.(io.Reader); ok {
		return rd
	}
	return &byteReaderWrapper{r}
}

type byteReaderWrapper struct{ r io.ByteReader }

func (w *byteReaderWrapper) Read(p []byte) (int, error) {
	for i := range p {
		b, err := w.r.ReadByte()
		if err != nil {
			return i, err
		}
		p[i] = b
	}
	return len(p), nil
}
