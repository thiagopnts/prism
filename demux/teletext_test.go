package demux

import (
	"math/bits"
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
	if g0Latin[0x23] != '£' {
		t.Errorf("g0Latin[0x23] = %c, want £", g0Latin[0x23])
	}
}

func TestNewTeletextDecoderFromDescriptors(t *testing.T) {
	t.Parallel()
	t.Run("with_subtitle_descriptor", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x56, Data: []byte{'e', 'n', 'g', 0x11, 0x88}}, // type=0x02 (subtitle), mag=1, page=0x88
		}
		td := newTeletextDecoderFromDescriptors(descs)
		if td == nil {
			t.Fatal("expected non-nil decoder")
		}
		if _, ok := td.subtitlePages[0x188]; !ok {
			t.Errorf("expected page 0x188 to be a target, got: %v", td.subtitlePages)
		}
	})

	t.Run("magazine_zero_means_eight", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x56, Data: []byte{'f', 'r', 'a', 0x10, 0x88}}, // type=0x02, mag=0 → 8, page=0x88
		}
		td := newTeletextDecoderFromDescriptors(descs)
		if td == nil {
			t.Fatal("expected non-nil decoder")
		}
		if _, ok := td.subtitlePages[0x888]; !ok {
			t.Errorf("expected page 0x888 to be a target, got: %v", td.subtitlePages)
		}
	})

	t.Run("non_subtitle_types_skipped", func(t *testing.T) {
		descs := []mpegts.PMTDescriptor{
			{Tag: 0x56, Data: []byte{'e', 'n', 'g', 0x19, 0x10}}, // type=0x03 (additional info)
		}
		td := newTeletextDecoderFromDescriptors(descs)
		if td != nil {
			t.Error("expected nil decoder when no subtitle pages declared")
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

	t.Run("no_descriptors", func(t *testing.T) {
		td := newTeletextDecoderFromDescriptors(nil)
		if td != nil {
			t.Error("expected nil decoder for nil descriptors")
		}
	})
}

// startPacket builds the common prefix of a 44-byte teletext data unit:
// framing code at buf[1] and Hamming-encoded magazine_and_packet_address at
// buf[2..3]. Returns the buffer for the caller to fill in bytes 4..43.
func startPacket(magazine, row uint8) [teletextDataUnitLength]byte {
	var buf [teletextDataUnitLength]byte
	buf[1] = 0xE4
	mag := magazine
	if mag == 8 {
		mag = 0
	}
	buf[2] = hammingEncode84((mag & 0x07) | ((row & 0x01) << 3))
	buf[3] = hammingEncode84((row >> 1) & 0x0F)
	return buf
}

func reverseBits(buf []byte) {
	for i, b := range buf {
		buf[i] = bits.Reverse8(b)
	}
}

// buildPageHeader constructs a row-0 (page header) data unit announcing the
// given BCD page on the magazine.
func buildPageHeader(magazine, page uint8) []byte {
	buf := startPacket(magazine, 0)
	buf[4] = hammingEncode84(page & 0x0F)
	buf[5] = hammingEncode84((page >> 4) & 0x0F)
	for i := 6; i < 12; i++ {
		buf[i] = hammingEncode84(0x00)
	}
	for i := 12; i < 4+teletextCharsPerRow && i < teletextDataUnitLength; i++ {
		buf[i] = addOddParity(' ')
	}
	reverseBits(buf[:])
	return buf[:]
}

// buildDisplayRow constructs a row-1..23 data unit with up to 40 chars of text.
func buildDisplayRow(magazine, row uint8, text string) []byte {
	buf := startPacket(magazine, row)
	for i := range teletextCharsPerRow {
		idx := 4 + i
		if idx >= teletextDataUnitLength {
			break
		}
		if i < len(text) {
			buf[idx] = addOddParity(text[i])
		} else {
			buf[idx] = addOddParity(' ')
		}
	}
	reverseBits(buf[:])
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

// decoderForPage returns a decoder configured to capture (magazine, page).
func decoderForPage(t *testing.T, magazine, page uint8) *teletextDecoder {
	t.Helper()
	mag := magazine
	if mag == 8 {
		mag = 0
	}
	descByte := (uint8(0x02) << 3) | (mag & 0x07)
	td := newTeletextDecoderFromDescriptors([]mpegts.PMTDescriptor{
		{Tag: 0x56, Data: []byte{'e', 'n', 'g', descByte, page}},
	})
	if td == nil {
		t.Fatalf("decoder construction failed for mag=%d page=0x%02x", magazine, page)
	}
	return td
}

func TestTeletextPageAssembly(t *testing.T) {
	t.Parallel()
	const targetPage uint8 = 0x88
	td := decoderForPage(t, 1, targetPage)

	page1Header := buildPageHeader(1, targetPage)
	page1Row21 := buildDisplayRow(1, 21, "Hello World")
	page1Row22 := buildDisplayRow(1, 22, "Second Line")

	pesData := buildTeletextPES(page1Header, page1Row21, page1Row22)
	results := td.processTeletextPES(pesData, 1000)

	if len(results) != 0 {
		t.Fatalf("expected 0 results before next header, got %d", len(results))
	}

	page2Header := buildPageHeader(1, targetPage)
	pesData2 := buildTeletextPES(page2Header)
	results = td.processTeletextPES(pesData2, 2000)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].pts != 1000 {
		t.Errorf("pts = %d, want 1000", results[0].pts)
	}
	text := linesText(results[0].lines)
	if !strings.Contains(text, "Hello World") {
		t.Errorf("text missing 'Hello World': %q", text)
	}
	if !strings.Contains(text, "Second Line") {
		t.Errorf("text missing 'Second Line': %q", text)
	}
}

func TestTeletextEmptyPageSkipped(t *testing.T) {
	t.Parallel()
	const targetPage uint8 = 0x88
	td := decoderForPage(t, 1, targetPage)

	header1 := buildPageHeader(1, targetPage)
	header2 := buildPageHeader(1, targetPage)
	pesData := buildTeletextPES(header1, header2)
	results := td.processTeletextPES(pesData, 1000)

	if len(results) != 0 {
		t.Errorf("expected 0 results for empty page, got %d", len(results))
	}
}

func TestTeletextNonTargetPagesIgnored(t *testing.T) {
	t.Parallel()
	const targetPage uint8 = 0x88
	td := decoderForPage(t, 1, targetPage)

	otherHeader := buildPageHeader(1, 0x99)
	row := buildDisplayRow(1, 21, "Should not appear")
	targetHeader := buildPageHeader(1, targetPage)

	pesData := buildTeletextPES(otherHeader, row, targetHeader)
	results := td.processTeletextPES(pesData, 1000)

	if len(results) != 0 {
		t.Fatalf("expected no emit when only non-target page seen, got %d", len(results))
	}
}

func TestTeletextShortPES(t *testing.T) {
	t.Parallel()
	td := decoderForPage(t, 1, 0x88)

	results := td.processTeletextPES(nil, 0)
	if len(results) != 0 {
		t.Errorf("expected 0 results for nil data, got %d", len(results))
	}

	results = td.processTeletextPES([]byte{0x10}, 0)
	if len(results) != 0 {
		t.Errorf("expected 0 results for data_identifier only, got %d", len(results))
	}
}

// linesText concatenates all span text across all lines into a single string
// with newlines between rows, for easy assertion in tests.
func linesText(lines []teletextLine) string {
	var parts []string
	for _, line := range lines {
		var sb strings.Builder
		for _, sp := range line.spans {
			sb.WriteString(sp.text)
		}
		parts = append(parts, sb.String())
	}
	return strings.Join(parts, "\n")
}

func TestTeletextNationalCharsets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		lang     string
		charCode byte   // 7-bit G0 code that has a national variant
		want     string // expected decoded character
	}{
		{"german_umlaut_Ä", "deu", 0x5B, "Ä"},
		{"german_umlaut_ö", "ger", 0x7C, "ö"},
		{"german_eszett", "deu", 0x7E, "ß"},
		{"swedish_Å", "swe", 0x5D, "Å"},
		{"finnish_ä", "fin", 0x7B, "ä"},
		{"hungarian_Ü", "hun", 0x5E, "Ü"},
		{"italian_à", "ita", 0x7B, "à"},
		{"italian_é", "ita", 0x40, "é"},
		{"spanish_ñ", "spa", 0x7C, "ñ"},
		{"portuguese_ç", "por", 0x23, "ç"},
		{"czech_š", "ces", 0x5C, "š"},
		{"slovak_ž", "slk", 0x5E, "ž"},
		{"french_ç", "fra", 0x7E, "ç"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mag := uint8(1)
			const page = uint8(0x88)
			descByte := (uint8(0x02) << 3) | (mag & 0x07)
			td := newTeletextDecoderFromDescriptors([]mpegts.PMTDescriptor{
				{Tag: 0x56, Data: []byte{tt.lang[0], tt.lang[1], tt.lang[2], descByte, page}},
			})
			if td == nil {
				t.Fatalf("decoder construction failed for lang=%s", tt.lang)
			}

			// Send page header to start collecting, then a row with the test character.
			header := buildPageHeader(mag, page)
			row := buildDisplayRow(mag, 21, string([]byte{tt.charCode}))
			pes := buildTeletextPES(header, row)
			td.processTeletextPES(pes, 1000)

			// Trigger emit with a second header.
			header2 := buildPageHeader(mag, page)
			results := td.processTeletextPES(buildTeletextPES(header2), 2000)
			if len(results) != 1 {
				t.Fatalf("expected 1 result, got %d", len(results))
			}
			text := linesText(results[0].lines)
			if !strings.Contains(text, tt.want) {
				t.Errorf("text %q does not contain %q", text, tt.want)
			}
		})
	}
}

func TestTeletextPerPageCharset(t *testing.T) {
	t.Parallel()
	// Two subtitle pages on the same PID: English (1/88) and German (2/89).
	// English page uses default charset; German page should use German subset.
	data := []byte{
		'e', 'n', 'g', 0x11, 0x88, // mag=1, page=0x88
		'g', 'e', 'r', 0x12, 0x89, // mag=2, page=0x89
	}
	entries := parseTeletextDescriptor(data)
	td := newTeletextDecoder(entries)
	if td == nil {
		t.Fatal("expected non-nil decoder")
	}

	// German page: 0x5B should decode as Ä (German) not ← (English default).
	engHeader := buildPageHeader(1, 0x88)
	gerHeader := buildPageHeader(2, 0x89)
	gerRow := buildDisplayRow(2, 21, string([]byte{0x5B})) // Ä in German, ← in English
	engHeader2 := buildPageHeader(1, 0x88)

	pes := buildTeletextPES(engHeader, gerHeader, gerRow, engHeader2)
	results := td.processTeletextPES(pes, 1000)

	// engHeader → starts collecting eng page (empty)
	// gerHeader → emits eng page (empty, skipped), starts collecting ger page
	// gerRow → appended to ger page
	// engHeader2 → emits ger page with 0x5B decoded as Ä
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	text := linesText(results[0].lines)
	if !strings.Contains(text, "Ä") {
		t.Errorf("German page: expected Ä, got %q", text)
	}
}

func TestTeletextErasePageFlag(t *testing.T) {
	t.Parallel()
	const targetPage uint8 = 0x88
	td := decoderForPage(t, 1, targetPage)

	// Start collecting, add a row, then send an erase page header.
	header := buildPageHeader(1, targetPage)
	row := buildDisplayRow(1, 21, "Some subtitle")
	eraseHeader := buildPageHeaderWithErase(1, targetPage, true)

	results := td.processTeletextPES(buildTeletextPES(header, row, eraseHeader), 1000)

	// Expect 2 outputs: the pending subtitle content, then the clear signal.
	if len(results) != 2 {
		t.Fatalf("expected 2 results (content + clear), got %d", len(results))
	}
	if len(results[0].lines) == 0 {
		t.Error("first result should contain the pending subtitle lines")
	}
	if len(results[1].lines) != 0 {
		t.Errorf("second result should be a clear signal (empty lines), got %d lines", len(results[1].lines))
	}
}

func TestTeletextEraseWithNoContent(t *testing.T) {
	t.Parallel()
	const targetPage uint8 = 0x88
	td := decoderForPage(t, 1, targetPage)

	// Erase arrives without any prior content (fresh decoder): only a clear.
	header := buildPageHeader(1, targetPage)
	eraseHeader := buildPageHeaderWithErase(1, targetPage, true)

	results := td.processTeletextPES(buildTeletextPES(header, eraseHeader), 1000)
	if len(results) != 1 {
		t.Fatalf("expected 1 clear result, got %d", len(results))
	}
	if len(results[0].lines) != 0 {
		t.Errorf("clear signal should have empty lines, got %d", len(results[0].lines))
	}
}

func TestTeletextRowNumbers(t *testing.T) {
	t.Parallel()
	const targetPage uint8 = 0x88
	td := decoderForPage(t, 1, targetPage)

	header := buildPageHeader(1, targetPage)
	row20 := buildDisplayRow(1, 20, "Bottom text")
	row21 := buildDisplayRow(1, 21, "Also bottom")
	header2 := buildPageHeader(1, targetPage)

	// First PES starts collection; second PES has the rows and triggers emit.
	td.processTeletextPES(buildTeletextPES(header), 500)
	results := td.processTeletextPES(buildTeletextPES(header, row20, row21, header2), 1000)

	if len(results) == 0 {
		t.Fatal("expected at least one result")
	}
	// Find the result with content.
	var found *teletextOutput
	for i := range results {
		if len(results[i].lines) > 0 {
			found = &results[i]
			break
		}
	}
	if found == nil {
		t.Fatal("no result with lines found")
	}
	if found.lines[0].rowNum != 20 {
		t.Errorf("first line rowNum = %d, want 20", found.lines[0].rowNum)
	}
	if found.lines[1].rowNum != 21 {
		t.Errorf("second line rowNum = %d, want 21", found.lines[1].rowNum)
	}
}

func TestTeletextColorSpans(t *testing.T) {
	t.Parallel()
	const targetPage uint8 = 0x88
	td := decoderForPage(t, 1, targetPage)

	// Build a row: yellow color code (0x03) followed by "Hello"
	header := buildPageHeader(1, targetPage)
	row := buildDisplayRow(1, 21, string([]byte{0x03, 'H', 'e', 'l', 'l', 'o'}))
	header2 := buildPageHeader(1, targetPage)

	td.processTeletextPES(buildTeletextPES(header, row), 1000)
	results := td.processTeletextPES(buildTeletextPES(header2), 2000)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	lines := results[0].lines
	if len(lines) == 0 {
		t.Fatal("expected at least one line")
	}

	// Find the span containing "Hello" — it should have yellow fg color.
	var helloSpan *teletextSpan
	for i := range lines[0].spans {
		if strings.Contains(lines[0].spans[i].text, "Hello") {
			helloSpan = &lines[0].spans[i]
			break
		}
	}
	if helloSpan == nil {
		t.Fatalf("span with 'Hello' not found in %+v", lines[0].spans)
	}
	if helloSpan.fgColor != teletextColors[3] {
		t.Errorf("fg color = %q, want %q (yellow)", helloSpan.fgColor, teletextColors[3])
	}
}

func TestTeletextMultiPageChannels(t *testing.T) {
	t.Parallel()
	// Two subtitle pages → channel 100 and 101.
	data := []byte{
		'e', 'n', 'g', 0x11, 0x88, // mag=1, page=0x88 → channel 100
		'g', 'e', 'r', 0x12, 0x89, // mag=2, page=0x89 → channel 101
	}
	td := newTeletextDecoder(parseTeletextDescriptor(data))
	if td == nil {
		t.Fatal("expected non-nil decoder")
	}

	page1Key := uint16(0x188)
	page2Key := uint16(0x289)
	ch1 := td.channelForPage(page1Key)
	ch2 := td.channelForPage(page2Key)
	if ch1 != 100 {
		t.Errorf("page 1 channel = %d, want 100", ch1)
	}
	if ch2 != 101 {
		t.Errorf("page 2 channel = %d, want 101", ch2)
	}
}

// buildPageHeaderWithErase constructs a row-0 data unit with the C5 (erase page)
// control bit optionally set. C5 is bit D3 (bit 2) of the Hamming-encoded byte
// at data[8] per EN 300 706 §9.3.1.
func buildPageHeaderWithErase(magazine, page uint8, erase bool) []byte {
	buf := startPacket(magazine, 0)
	buf[4] = hammingEncode84(page & 0x0F)
	buf[5] = hammingEncode84((page >> 4) & 0x0F)
	buf[6] = hammingEncode84(0x00) // S1
	buf[7] = hammingEncode84(0x00) // S2 + C4
	// byte[8]: D1=S3[0], D2=S3[1], D3=C5, D4=C6
	c5nibble := uint8(0x00)
	if erase {
		c5nibble = 0x04 // bit D3 set = C5 = 1
	}
	buf[8] = hammingEncode84(c5nibble)
	buf[9] = hammingEncode84(0x00)
	buf[10] = hammingEncode84(0x00)
	buf[11] = hammingEncode84(0x00)
	for i := 12; i < 4+teletextCharsPerRow && i < teletextDataUnitLength; i++ {
		buf[i] = addOddParity(' ')
	}
	reverseBits(buf[:])
	return buf[:]
}
