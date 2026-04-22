package demux

import (
	"strings"
	"testing"

	"github.com/zsiec/prism/mpegts"
)

func TestParseTeletextDescriptor(t *testing.T) {
	t.Parallel()
	data := []byte{
		'e', 'n', 'g', 0x11, 0x08, // type=0x02, magazine=1, page=0x08
		'f', 'r', 'a', 0x2A, 0x89, // type=0x05, magazine=2, page=0x89
	}
	entries := parseTeletextDescriptor(data)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].languageCode != "eng" {
		t.Errorf("entry 0 lang = %q, want \"eng\"", entries[0].languageCode)
	}
	if entries[0].teletextType != 0x02 {
		t.Errorf("entry 0 type = 0x%02X, want 0x02", entries[0].teletextType)
	}
	if entries[0].magazine != 1 {
		t.Errorf("entry 0 magazine = %d, want 1", entries[0].magazine)
	}
	if entries[0].page != 0x08 {
		t.Errorf("entry 0 page = 0x%02X, want 0x08", entries[0].page)
	}
	if entries[1].languageCode != "fra" {
		t.Errorf("entry 1 lang = %q, want \"fra\"", entries[1].languageCode)
	}
	if entries[1].teletextType != 0x05 {
		t.Errorf("entry 1 type = 0x%02X, want 0x05", entries[1].teletextType)
	}
}

func TestParseTeletextDescriptor_ShortData(t *testing.T) {
	t.Parallel()
	entries := parseTeletextDescriptor([]byte{0x01, 0x02, 0x03})
	if len(entries) != 0 {
		t.Errorf("expected 0 entries for short data, got %d", len(entries))
	}
}

func TestHammingDecode84(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   byte
		want    byte
		wantErr bool
	}{
		{"zero_encoded", 0x15, 0x00, false},
		{"one_encoded", 0x02, 0x01, false},
		{"error_byte", 0x01, 0xFF, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hammingDecode84[tt.input]
			if tt.wantErr && got != 0xFF {
				t.Errorf("expected error (0xFF), got 0x%02X", got)
			} else if !tt.wantErr && got != tt.want {
				t.Errorf("got 0x%02X, want 0x%02X", got, tt.want)
			}
		})
	}
}

func TestBitReverse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input byte
		want  byte
	}{
		{0x00, 0x00},
		{0xFF, 0xFF},
		{0x80, 0x01},
		{0x01, 0x80},
		{0xA5, 0xA5},
	}
	for _, tt := range tests {
		if got := bitReverse[tt.input]; got != tt.want {
			t.Errorf("bitReverse[0x%02X] = 0x%02X, want 0x%02X", tt.input, got, tt.want)
		}
	}
}

func TestG0LatinDecode(t *testing.T) {
	t.Parallel()
	if g0Latin[0x41] != 'A' {
		t.Errorf("g0Latin[0x41] = %c, want A", g0Latin[0x41])
	}
	if g0Latin[0x20] != ' ' {
		t.Errorf("g0Latin[0x20] = %c, want space", g0Latin[0x20])
	}
	if g0Latin[0x00] != ' ' {
		t.Errorf("g0Latin[0x00] = %c, want space (spacing attr)", g0Latin[0x00])
	}
	if g0Latin[0x23] != '\u00a3' {
		t.Errorf("g0Latin[0x23] = %c, want £", g0Latin[0x23])
	}
}

func TestNewTeletextDecoderFromDescriptors(t *testing.T) {
	t.Parallel()
	t.Run("with_teletext_descriptor", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x56, Data: []byte{'e', 'n', 'g', 0x11, 0x08}},
		}
		td := newTeletextDecoderFromDescriptors(descs)
		if td == nil {
			t.Error("expected non-nil decoder")
		}
	})

	t.Run("without_teletext_descriptor", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x59, Data: []byte{0x01, 0x02}},
		}
		td := newTeletextDecoderFromDescriptors(descs)
		if td != nil {
			t.Error("expected nil decoder for non-teletext descriptor")
		}
	})

	t.Run("empty_teletext_descriptor", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x56, Data: []byte{}},
		}
		td := newTeletextDecoderFromDescriptors(descs)
		if td != nil {
			t.Error("expected nil decoder for empty teletext descriptor")
		}
	})

	t.Run("no_descriptors", func(t *testing.T) {
		td := newTeletextDecoderFromDescriptors(nil)
		if td != nil {
			t.Error("expected nil decoder for nil descriptors")
		}
	})
}

func buildTeletextPacket(magazine, row uint8, text string) []byte {
	var buf [teletextDataUnitLength]byte

	buf[2] = teletextFramingCode

	mag := magazine
	if mag == 8 {
		mag = 0
	}
	addr1 := (mag & 0x07) | ((row & 0x01) << 3)
	addr2 := row >> 1
	buf[3] = hammingEncode84(addr1)
	buf[4] = hammingEncode84(addr2)

	if row == 0 {
		buf[5] = hammingEncode84(0x00)
		buf[6] = hammingEncode84(0x01)
		for i := 7; i < 13; i++ {
			buf[i] = hammingEncode84(0x00)
		}
		for i := 13; i < 5+teletextCharsPerRow && i < teletextDataUnitLength; i++ {
			buf[i] = addOddParity(' ')
		}
	} else {
		for i := range teletextCharsPerRow {
			idx := 5 + i
			if idx >= teletextDataUnitLength {
				break
			}
			if i < len(text) {
				buf[idx] = addOddParity(text[i])
			} else {
				buf[idx] = addOddParity(' ')
			}
		}
	}

	for i := range buf {
		buf[i] = bitReverse[buf[i]]
	}
	return buf[:]
}

var hammingEncode84Table = [16]byte{
	0x15, 0x02, 0x49, 0x5E, 0x64, 0x73, 0x38, 0x2F,
	0xD0, 0xC7, 0x8C, 0x9B, 0xA1, 0xB6, 0xFD, 0xEA,
}

func hammingEncode84(nibble byte) byte {
	return hammingEncode84Table[nibble&0x0F]
}

func addOddParity(ch byte) byte {
	b := ch & 0x7F
	ones := 0
	for i := range 7 {
		if b&(1<<i) != 0 {
			ones++
		}
	}
	if ones%2 == 0 {
		b |= 0x80
	}
	return b
}

func buildTeletextPES(packets ...[]byte) []byte {
	pes := []byte{0x10}
	for _, pkt := range packets {
		pes = append(pes, 0x03)
		pes = append(pes, teletextDataUnitLength)
		pes = append(pes, pkt...)
	}
	return pes
}

func TestTeletextPageAssembly(t *testing.T) {
	t.Parallel()
	td := newTeletextDecoder()

	page1Header := buildTeletextPacket(1, 0, "")
	page1Row21 := buildTeletextPacket(1, 21, "Hello World")
	page1Row22 := buildTeletextPacket(1, 22, "Second Line")

	pesData := buildTeletextPES(page1Header, page1Row21, page1Row22)
	results := td.processTeletextPES(pesData, 1000)

	if len(results) != 0 {
		t.Fatalf("expected 0 results before next header, got %d", len(results))
	}

	page2Header := buildTeletextPacket(1, 0, "")
	pesData2 := buildTeletextPES(page2Header)
	results = td.processTeletextPES(pesData2, 2000)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].pts != 1000 {
		t.Errorf("pts = %d, want 1000", results[0].pts)
	}
	text := strings.Join(results[0].lines, "\n")
	if !strings.Contains(text, "Hello World") {
		t.Errorf("text missing 'Hello World': %q", text)
	}
	if !strings.Contains(text, "Second Line") {
		t.Errorf("text missing 'Second Line': %q", text)
	}
}

func TestTeletextEmptyPageSkipped(t *testing.T) {
	t.Parallel()
	td := newTeletextDecoder()

	header1 := buildTeletextPacket(1, 0, "")
	header2 := buildTeletextPacket(1, 0, "")
	pesData := buildTeletextPES(header1, header2)
	results := td.processTeletextPES(pesData, 1000)

	if len(results) != 0 {
		t.Errorf("expected 0 results for empty page, got %d", len(results))
	}
}

func TestTeletextMultipleMagazines(t *testing.T) {
	t.Parallel()
	td := newTeletextDecoder()

	mag1Header := buildTeletextPacket(1, 0, "")
	mag2Header := buildTeletextPacket(2, 0, "")
	mag1Row := buildTeletextPacket(1, 21, "Magazine 1")
	mag2Row := buildTeletextPacket(2, 21, "Magazine 2")

	pesData := buildTeletextPES(mag1Header, mag2Header, mag1Row, mag2Row)
	_ = td.processTeletextPES(pesData, 1000)

	mag1Header2 := buildTeletextPacket(1, 0, "")
	mag2Header2 := buildTeletextPacket(2, 0, "")
	pesData2 := buildTeletextPES(mag1Header2, mag2Header2)
	results := td.processTeletextPES(pesData2, 2000)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	texts := make(map[string]bool)
	for _, r := range results {
		texts[strings.Join(r.lines, "\n")] = true
	}
	if !texts["Magazine 1"] {
		t.Error("missing 'Magazine 1' text")
	}
	if !texts["Magazine 2"] {
		t.Error("missing 'Magazine 2' text")
	}
}

func TestTeletextBadFramingCode(t *testing.T) {
	t.Parallel()
	td := newTeletextDecoder()

	pkt := buildTeletextPacket(1, 0, "")
	pkt[2] = 0x00

	pesData := buildTeletextPES(pkt)
	results := td.processTeletextPES(pesData, 1000)
	if len(results) != 0 {
		t.Errorf("expected 0 results for bad framing code, got %d", len(results))
	}
}

func TestTeletextShortPES(t *testing.T) {
	t.Parallel()
	td := newTeletextDecoder()

	results := td.processTeletextPES(nil, 0)
	if len(results) != 0 {
		t.Errorf("expected 0 results for nil data, got %d", len(results))
	}

	results = td.processTeletextPES([]byte{0x10}, 0)
	if len(results) != 0 {
		t.Errorf("expected 0 results for data_identifier only, got %d", len(results))
	}
}
