package moqclient

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/quic-go/quic-go/quicvarint"
)

// appendVarint appends a QUIC varint to b.
func appendVarint(b []byte, v uint64) []byte { return quicvarint.Append(b, v) }

// buildSubgroupHeader encodes a subgroup header into bytes.
func buildSubgroupHeader(trackAlias, groupID, subgroupID uint64, priority byte) []byte {
	var b []byte
	b = appendVarint(b, StreamTypeSubgroupSIDExt)
	b = appendVarint(b, trackAlias)
	b = appendVarint(b, groupID)
	b = appendVarint(b, subgroupID)
	b = append(b, priority)
	return b
}

// buildObject encodes a single object with raw extensions bytes and payload.
func buildObject(objectID uint64, extsBytes []byte, payload []byte) []byte {
	var b []byte
	b = appendVarint(b, objectID)
	b = appendVarint(b, uint64(len(extsBytes)))
	b = append(b, extsBytes...)
	b = appendVarint(b, uint64(len(payload)))
	b = append(b, payload...)
	return b
}

// buildExtEven encodes one even-parity extension (varint value).
func buildExtEven(id, value uint64) []byte {
	var b []byte
	b = appendVarint(b, id)
	b = appendVarint(b, value)
	return b
}

// buildExtOdd encodes one odd-parity extension (length-prefixed bytes).
func buildExtOdd(id uint64, data []byte) []byte {
	var b []byte
	b = appendVarint(b, id)
	b = appendVarint(b, uint64(len(data)))
	b = append(b, data...)
	return b
}

func mkReader(data []byte) io.ByteReader {
	return bufio.NewReader(bytes.NewReader(data))
}

// --- SubgroupHeader tests ---

func TestReadSubgroupHeader(t *testing.T) {
	tests := []struct {
		name       string
		build      func() []byte
		want       SubgroupHeader
		wantErrSub string
	}{
		{
			name: "typical",
			build: func() []byte {
				return buildSubgroupHeader(42, 7, 0, 128)
			},
			want: SubgroupHeader{TrackAlias: 42, GroupID: 7, SubgroupID: 0, Priority: 128},
		},
		{
			name: "zero fields",
			build: func() []byte {
				return buildSubgroupHeader(0, 0, 0, 0)
			},
			want: SubgroupHeader{},
		},
		{
			name: "wrong stream type",
			build: func() []byte {
				var b []byte
				b = appendVarint(b, 0x42) // wrong type
				b = appendVarint(b, 1)
				b = appendVarint(b, 0)
				b = appendVarint(b, 0)
				b = append(b, 0)
				return b
			},
			wantErrSub: "unexpected stream type",
		},
		{
			name: "truncated after stream type",
			build: func() []byte {
				return appendVarint(nil, StreamTypeSubgroupSIDExt)
			},
			wantErrSub: "track alias",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadSubgroupHeader(mkReader(tt.build()))
			if tt.wantErrSub != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErrSub)
				}
				if !bytes.Contains([]byte(err.Error()), []byte(tt.wantErrSub)) {
					t.Fatalf("error = %q, want it to contain %q", err.Error(), tt.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadSubgroupHeader: %v", err)
			}
			if got != tt.want {
				t.Fatalf("header = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// --- Object tests ---

func TestReadObject_Empty(t *testing.T) {
	// empty stream → EOF
	if _, err := ReadObject(mkReader(nil)); err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestReadObject_NoExtensions(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	data := buildObject(0, nil, payload)

	obj, err := ReadObject(mkReader(data))
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if obj.ObjectID != 0 {
		t.Errorf("ObjectID = %d, want 0", obj.ObjectID)
	}
	if len(obj.Extensions) != 0 {
		t.Errorf("Extensions = %v, want empty", obj.Extensions)
	}
	if obj.RawExtBytes != nil {
		t.Errorf("RawExtBytes = %v, want nil for an object with no extensions", obj.RawExtBytes)
	}
	if !bytes.Equal(obj.Payload, payload) {
		t.Errorf("Payload = %v, want %v", obj.Payload, payload)
	}
}

func TestReadObject_VideoKeyframe(t *testing.T) {
	configData := []byte{0xAA, 0xBB, 0xCC}
	avc1Payload := []byte{0x00, 0x00, 0x00, 0x05, 0x65, 0x01, 0x02, 0x03, 0x04}

	var extsBytes []byte
	extsBytes = append(extsBytes, buildExtEven(ExtCaptureTimestamp, 1234567)...)
	extsBytes = append(extsBytes, buildExtEven(ExtVideoFrameMarking, VFMKeyframe)...)
	extsBytes = append(extsBytes, buildExtOdd(ExtVideoConfig, configData)...)

	data := buildObject(5, extsBytes, avc1Payload)

	obj, err := ReadObject(mkReader(data))
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if obj.ObjectID != 5 {
		t.Errorf("ObjectID = %d, want 5", obj.ObjectID)
	}
	if !bytes.Equal(obj.Payload, avc1Payload) {
		t.Errorf("Payload = %v, want %v", obj.Payload, avc1Payload)
	}
	if CaptureTimestampUS(obj) != 1234567 {
		t.Errorf("CaptureTimestampUS = %d, want 1234567", CaptureTimestampUS(obj))
	}
	if !IsKeyframe(obj) {
		t.Error("IsKeyframe = false, want true")
	}
	if !bytes.Equal(VideoConfigBytes(obj), configData) {
		t.Errorf("VideoConfigBytes = %v, want %v", VideoConfigBytes(obj), configData)
	}
}

func TestReadObject_VideoDeltaFrame(t *testing.T) {
	var extsBytes []byte
	extsBytes = append(extsBytes, buildExtEven(ExtCaptureTimestamp, 9999)...)
	extsBytes = append(extsBytes, buildExtEven(ExtVideoFrameMarking, 0xC0)...) // non-keyframe

	data := buildObject(1, extsBytes, []byte{0x11})

	obj, err := ReadObject(mkReader(data))
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if CaptureTimestampUS(obj) != 9999 {
		t.Errorf("CaptureTimestampUS = %d, want 9999", CaptureTimestampUS(obj))
	}
	if IsKeyframe(obj) {
		t.Error("IsKeyframe = true, want false")
	}
	if VideoConfigBytes(obj) != nil {
		t.Errorf("VideoConfigBytes = %v, want nil", VideoConfigBytes(obj))
	}
}

func TestReadObject_AudioFrame(t *testing.T) {
	audioPayload := []byte{0x11, 0x90, 0x40, 0x00}
	var extsBytes []byte
	extsBytes = append(extsBytes, buildExtEven(ExtCaptureTimestamp, 48000*1000)...)

	data := buildObject(0, extsBytes, audioPayload)

	obj, err := ReadObject(mkReader(data))
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if !bytes.Equal(obj.Payload, audioPayload) {
		t.Errorf("Payload = %v, want %v", obj.Payload, audioPayload)
	}
	if CaptureTimestampUS(obj) != 48000*1000 {
		t.Errorf("CaptureTimestampUS = %d, want %d", CaptureTimestampUS(obj), 48000*1000)
	}
	if IsKeyframe(obj) {
		t.Error("IsKeyframe = true, want false")
	}
}

func TestReadObject_UnknownExtensionSkipped(t *testing.T) {
	var extsBytes []byte
	extsBytes = append(extsBytes, buildExtEven(100, 42)...)          // unknown even
	extsBytes = append(extsBytes, buildExtOdd(101, []byte{0x01})...) // unknown odd
	extsBytes = append(extsBytes, buildExtEven(ExtCaptureTimestamp, 7777)...)

	data := buildObject(0, extsBytes, []byte{0xFF})

	obj, err := ReadObject(mkReader(data))
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	// Should have found capture timestamp despite unknown extensions before it.
	if CaptureTimestampUS(obj) != 7777 {
		t.Errorf("CaptureTimestampUS = %d, want 7777", CaptureTimestampUS(obj))
	}
	// RawExtBytes must retain the full block verbatim, including the unknown
	// extensions, so a relay can forward it unchanged.
	if !bytes.Equal(obj.RawExtBytes, extsBytes) {
		t.Errorf("RawExtBytes = %v, want %v (verbatim, incl. unknown extensions)", obj.RawExtBytes, extsBytes)
	}
}

// TestReadObject_RawExtBytesVerbatim asserts the raw extension block is captured
// byte-for-byte so a relay can re-serve it without re-encoding.
func TestReadObject_RawExtBytesVerbatim(t *testing.T) {
	var extsBytes []byte
	extsBytes = append(extsBytes, buildExtEven(ExtCaptureTimestamp, 1234567)...)
	extsBytes = append(extsBytes, buildExtOdd(ExtVideoConfig, []byte{0xDE, 0xAD, 0xBE, 0xEF})...)

	data := buildObject(9, extsBytes, []byte{0x01, 0x02})

	obj, err := ReadObject(mkReader(data))
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if !bytes.Equal(obj.RawExtBytes, extsBytes) {
		t.Fatalf("RawExtBytes = %v, want %v", obj.RawExtBytes, extsBytes)
	}
	// Re-prefixing the raw block with its varint length reproduces the original
	// on-wire extension framing exactly.
	rebuilt := appendVarint(nil, uint64(len(obj.RawExtBytes)))
	rebuilt = append(rebuilt, obj.RawExtBytes...)
	want := appendVarint(nil, uint64(len(extsBytes)))
	want = append(want, extsBytes...)
	if !bytes.Equal(rebuilt, want) {
		t.Fatalf("rebuilt ext framing = %v, want %v", rebuilt, want)
	}
}

func TestReadObject_TruncatedPayload(t *testing.T) {
	var b []byte
	b = appendVarint(b, 0)         // object ID
	b = appendVarint(b, 0)         // ext byte len
	b = appendVarint(b, 100)       // claim 100 bytes payload
	b = append(b, []byte{0x01}...) // only 1 byte

	if _, err := ReadObject(mkReader(b)); err == nil {
		t.Fatal("ReadObject should error on a truncated payload")
	}
}

func TestReadObject_ExtByteCountMismatch(t *testing.T) {
	// claim 10 extension bytes but only provide 2
	var b []byte
	b = appendVarint(b, 0) // object ID
	b = appendVarint(b, 10)
	b = append(b, 0x00, 0x00) // only 2 bytes

	if _, err := ReadObject(mkReader(b)); err == nil {
		t.Fatal("ReadObject should error when the extension block is truncated")
	}
}

// TestReadMultipleObjects ensures ReadObject can be called sequentially.
func TestReadMultipleObjects(t *testing.T) {
	var data []byte
	for i := 0; i < 3; i++ {
		var exts []byte
		exts = append(exts, buildExtEven(ExtCaptureTimestamp, uint64(i*1000))...)
		data = append(data, buildObject(uint64(i), exts, []byte{byte(i)})...)
	}

	r := mkReader(data)
	for i := 0; i < 3; i++ {
		obj, err := ReadObject(r)
		if err != nil {
			t.Fatalf("ReadObject #%d: %v", i, err)
		}
		if obj.ObjectID != uint64(i) {
			t.Errorf("ObjectID = %d, want %d", obj.ObjectID, i)
		}
		if CaptureTimestampUS(obj) != uint64(i*1000) {
			t.Errorf("CaptureTimestampUS = %d, want %d", CaptureTimestampUS(obj), i*1000)
		}
	}
	if _, err := ReadObject(r); err != io.EOF {
		t.Fatalf("err = %v, want io.EOF after the last object", err)
	}
}
