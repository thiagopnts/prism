package demux

import (
	"math/bits"
	"strings"
	"unicode/utf8"

	"github.com/zsiec/prism/mpegts"
)

// EBU/DVB teletext (EN 300 472) constants. Within a 44-byte data unit
// the structure is:
//
//	[0]      reserved + field_parity + line_offset
//	[1]      framing code (0xE4 on the wire; 0x27 after bit-reversal)
//	[2..3]   magazine_and_packet_address (Hamming 8/4)
//	[4..43]  data block — 40 bytes of character or control data
//
// All bytes are transmitted LSB first; we bit-reverse every byte before
// interpreting it.
const (
	streamTypePrivateData  = 0x06
	descriptorTagTeletext  = 0x56
	teletextDataUnitLength = 44
	teletextPageRows       = 25
	teletextCharsPerRow    = 40
	teletextChannel        = 100 // distinguishes teletext from CEA-608 (1-4) and CEA-708 (7-12)

	colorBlack = "000000"
	colorWhite = "ffffff"
)

// teletextColors maps a 3-bit teletext color index (0=black … 7=white) to a
// 6-digit hex RGB string, per EN 300 706 Table 26.
var teletextColors = [8]string{
	colorBlack,
	"ff0000",
	"00ff00",
	"ffff00",
	"0000ff",
	"ff00ff",
	"00ffff",
	colorWhite,
}

type teletextDescriptorEntry struct {
	languageCode string
	teletextType uint8 // 0x02=subtitle, 0x03=additional info, 0x05=HoH subtitle
	magazine     uint8
	page         uint8 // BCD encoded
}

func parseTeletextDescriptor(data []byte) []teletextDescriptorEntry {
	var entries []teletextDescriptorEntry
	for i := 0; i+5 <= len(data); i += 5 {
		entries = append(entries, teletextDescriptorEntry{
			languageCode: string(data[i : i+3]),
			teletextType: data[i+3] >> 3,
			magazine:     data[i+3] & 0x07,
			page:         data[i+4],
		})
	}
	return entries
}

// teletextSpan is a run of characters sharing the same foreground/background
// color within a single teletext display row.
type teletextSpan struct {
	text    string
	fgColor string // 6-hex RGB, e.g. "ffffff"
	bgColor string
}

// teletextPage accumulates display rows for a single in-progress teletext page.
type teletextPage struct {
	rows [teletextPageRows][]teletextSpan
	pts  int64
}

// teletextLine is a single non-empty display row ready for output.
type teletextLine struct {
	spans  []teletextSpan
	rowNum int
}

type teletextOutput struct {
	lines []teletextLine
	pts   int64
	page  uint16 // source page (mag<<8 | bcd) the lines/erase were collected for
}

type teletextDecoder struct {
	// subtitlePages maps page key (mag<<8|bcd) to the G0 charset for that page.
	// Only subtitle (0x02) and HoH (0x05) pages are stored.
	subtitlePages map[uint16][128]rune
	// pageChannels maps page key to a per-page channel offset (0-based).
	// Channel = teletextChannel + pageChannels[page].
	pageChannels map[uint16]int

	collecting     bool
	currentPage    uint16
	currentCharset [128]rune
	page           teletextPage
}

func newTeletextDecoder(entries []teletextDescriptorEntry) *teletextDecoder {
	td := &teletextDecoder{
		subtitlePages: map[uint16][128]rune{},
		pageChannels:  map[uint16]int{},
	}
	for _, e := range entries {
		// teletextType 0x02 (subtitle) and 0x05 (HoH subtitle) carry captions.
		if e.teletextType != 0x02 && e.teletextType != 0x05 {
			continue
		}
		mag := uint16(e.magazine)
		if mag == 0 {
			mag = 8
		}
		pageKey := (mag << 8) | uint16(e.page)

		charset := g0Latin
		if subset, ok := nationalSubsets[e.languageCode]; ok {
			for i, pos := range nationalPositions {
				charset[pos] = subset[i]
			}
		}
		td.subtitlePages[pageKey] = charset
		td.pageChannels[pageKey] = len(td.pageChannels)
	}
	if len(td.subtitlePages) == 0 {
		return nil
	}
	return td
}

func newTeletextDecoderFromDescriptors(descs []mpegts.PMTDescriptor) *teletextDecoder {
	for _, d := range descs {
		if d.Tag == descriptorTagTeletext {
			if td := newTeletextDecoder(parseTeletextDescriptor(d.Data)); td != nil {
				return td
			}
		}
	}
	return nil
}

func (td *teletextDecoder) processTeletextPES(pesData []byte, pts int64) []teletextOutput {
	if len(pesData) < 1 {
		return nil
	}

	// Skip data_identifier byte
	offset := 1

	var results []teletextOutput

	for offset+2+teletextDataUnitLength <= len(pesData) {
		dataUnitID := pesData[offset]
		dataUnitLen := pesData[offset+1]
		offset += 2

		if int(dataUnitLen) != teletextDataUnitLength || offset+teletextDataUnitLength > len(pesData) {
			offset += int(dataUnitLen)
			continue
		}

		if dataUnitID == 0x02 || dataUnitID == 0x03 {
			results = append(results, td.processTeletextPacket(pesData[offset:offset+teletextDataUnitLength], pts)...)
		}

		offset += teletextDataUnitLength
	}

	return results
}

func (td *teletextDecoder) processTeletextPacket(data []byte, pts int64) []teletextOutput {
	if len(data) < teletextDataUnitLength {
		return nil
	}

	b2 := hammingDecode84[bits.Reverse8(data[2])]
	b3 := hammingDecode84[bits.Reverse8(data[3])]
	if b2 == 0xFF || b3 == 0xFF {
		return nil
	}

	addr := (b3 << 4) | b2
	magazine := addr & 0x07
	if magazine == 0 {
		magazine = 8
	}
	row := (addr >> 3) & 0x1f

	switch {
	case row == 0:
		return td.handlePageHeader(data, magazine, pts)
	case row >= 1 && row <= 23 && td.collecting:
		td.handleDisplayRow(data, row)
	}
	return nil
}

func (td *teletextDecoder) handlePageHeader(data []byte, magazine uint8, pts int64) []teletextOutput {
	pu := hammingDecode84[bits.Reverse8(data[4])]
	pt := hammingDecode84[bits.Reverse8(data[5])]
	if pu == 0xFF || pt == 0xFF {
		return nil
	}
	page := (uint16(magazine) << 8) | (uint16(pt) << 4) | uint16(pu)

	// Decode C5 (erase page) from control byte [8]:
	// data[8] is the third Hamming-protected control byte; after decoding,
	// D3 (bit 2) = C5 per EN 300 706 §9.3.1 Table 3.
	erasePage := false
	if cb := hammingDecode84[bits.Reverse8(data[8])]; cb != 0xFF {
		erasePage = (cb>>2)&1 == 1
	}

	var outputs []teletextOutput

	// Emit the previously collected page (if any) before starting the new one.
	if td.collecting {
		lines := extractPageLines(&td.page)
		if len(lines) > 0 {
			outputs = append(outputs, teletextOutput{lines: lines, pts: td.page.pts, page: td.currentPage})
		}
	}

	charset, isSubtitle := td.subtitlePages[page]
	td.collecting = isSubtitle && !erasePage
	td.currentPage = page
	if td.collecting {
		td.currentCharset = charset
	}
	td.page = teletextPage{pts: pts}

	// Erase page: append an explicit empty output so the caller can clear the
	// subtitle display. This follows any pending content already appended above.
	if isSubtitle && erasePage {
		outputs = append(outputs, teletextOutput{pts: pts, page: page})
	}

	return outputs
}

func (td *teletextDecoder) handleDisplayRow(data []byte, row byte) {
	if int(row) >= teletextPageRows {
		return
	}

	curFg := colorWhite
	curBg := colorBlack

	var spans []teletextSpan
	var sb strings.Builder
	sb.Grow(teletextCharsPerRow * utf8.UTFMax)

	flushSpan := func() {
		text := sb.String()
		sb.Reset()
		if text != "" {
			spans = append(spans, teletextSpan{text: text, fgColor: curFg, bgColor: curBg})
		}
	}

	for i := 4; i < 4+teletextCharsPerRow && i < len(data); i++ {
		ch := bits.Reverse8(data[i]) & 0x7F

		var newFg, newBg string
		isAttr := true
		switch {
		case ch <= 0x07:
			newFg, newBg = teletextColors[ch], curBg
		case ch >= 0x10 && ch <= 0x17:
			newFg, newBg = curFg, teletextColors[ch&0x07]
		case ch == 0x1C:
			newFg, newBg = curFg, colorBlack
		case ch == 0x1D:
			newFg, newBg = curFg, curFg
		default:
			isAttr = false
			if ch < 0x20 {
				sb.WriteRune(' ') // other control codes → blank position
			} else {
				sb.WriteRune(td.currentCharset[ch])
			}
		}
		if isAttr {
			// Control codes occupy a blank character position in the grid.
			flushSpan()
			curFg, curBg = newFg, newBg
			sb.WriteRune(' ')
		}
	}
	flushSpan()

	td.page.rows[row] = spans
}

// extractPageLines returns non-empty display rows (1–23) with their row numbers,
// trimming trailing-space spans.
func extractPageLines(page *teletextPage) []teletextLine {
	var lines []teletextLine
	for rowIdx := 1; rowIdx <= 23; rowIdx++ {
		if trimmed := trimTrailingSpaces(page.rows[rowIdx]); len(trimmed) > 0 {
			lines = append(lines, teletextLine{spans: trimmed, rowNum: rowIdx})
		}
	}
	return lines
}

// trimTrailingSpaces removes trailing-whitespace-only spans and trims trailing
// spaces from the last non-empty span. Returns the original slice unchanged when
// no trimming is needed (avoids allocation in the common case).
func trimTrailingSpaces(spans []teletextSpan) []teletextSpan {
	if len(spans) == 0 {
		return nil
	}
	end := len(spans)
	for end > 0 {
		t := strings.TrimRight(spans[end-1].text, " ")
		if t != "" {
			if end == len(spans) && t == spans[end-1].text {
				return spans // nothing to trim
			}
			result := make([]teletextSpan, end)
			copy(result, spans[:end])
			result[end-1].text = t
			return result
		}
		end--
	}
	return nil
}

func (td *teletextDecoder) channelForPage(page uint16) int {
	return teletextChannel + td.pageChannels[page]
}
