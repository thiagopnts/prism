package demux

import (
	"strings"

	"github.com/zsiec/prism/mpegts"
)

const (
	streamTypePrivateData  = 0x06
	descriptorTagTeletext  = 0x56
	teletextFramingCode    = 0xE4
	teletextDataUnitLength = 44
	teletextPageRows       = 25
	teletextCharsPerRow    = 40
	teletextChannel        = 100 // distinguishes teletext from CEA-608 (1-4) and CEA-708 (7-12)
)

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

type teletextPage struct {
	rows [teletextPageRows]string
	pts  int64
}

type teletextOutput struct {
	lines []string
	pts   int64
}

type teletextDecoder struct {
	pages map[uint8]*teletextPage
}

func newTeletextDecoder() *teletextDecoder {
	return &teletextDecoder{
		pages: make(map[uint8]*teletextPage),
	}
}

func newTeletextDecoderFromDescriptors(descs []mpegts.PMTDescriptor) *teletextDecoder {
	for _, d := range descs {
		if d.Tag == descriptorTagTeletext {
			entries := parseTeletextDescriptor(d.Data)
			if len(entries) > 0 {
				return newTeletextDecoder()
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
			if out := td.processTeletextPacket(pesData[offset:offset+teletextDataUnitLength], pts); out != nil {
				results = append(results, *out)
			}
		}

		offset += teletextDataUnitLength
	}

	return results
}

func (td *teletextDecoder) processTeletextPacket(data []byte, pts int64) *teletextOutput {
	if len(data) < teletextDataUnitLength {
		return nil
	}

	var buf [teletextDataUnitLength]byte
	for i, b := range data[:teletextDataUnitLength] {
		buf[i] = bitReverse[b]
	}

	if buf[2] != teletextFramingCode {
		return nil
	}

	b3 := hammingDecode84[buf[3]]
	b4 := hammingDecode84[buf[4]]
	if b3 == 0xFF || b4 == 0xFF {
		return nil
	}

	magazine := b3 & 0x07
	if magazine == 0 {
		magazine = 8
	}
	row := (b3 >> 3) | (b4 << 1)

	if row == 0 {
		return td.handlePageHeader(magazine, pts)
	}

	if row >= 1 && row <= 23 {
		td.handleDisplayRow(buf[:], magazine, row)
	}

	return nil
}

func (td *teletextDecoder) handlePageHeader(magazine uint8, pts int64) *teletextOutput {
	var result *teletextOutput

	if prev, ok := td.pages[magazine]; ok {
		lines := extractPageLines(prev)
		if len(lines) > 0 {
			result = &teletextOutput{lines: lines, pts: prev.pts}
		}
	}

	td.pages[magazine] = &teletextPage{pts: pts}

	return result
}

func (td *teletextDecoder) handleDisplayRow(buf []byte, magazine uint8, row byte) {
	page, ok := td.pages[magazine]
	if !ok {
		return
	}

	if int(row) >= teletextPageRows {
		return
	}

	var sb strings.Builder
	sb.Grow(teletextCharsPerRow)
	for i := 5; i < 5+teletextCharsPerRow && i < len(buf); i++ {
		ch := buf[i] & 0x7F
		sb.WriteRune(g0Latin[ch])
	}
	page.rows[row] = sb.String()
}

// extractPageLines returns non-empty display rows (1-23), trimming trailing spaces.
func extractPageLines(page *teletextPage) []string {
	var lines []string
	for _, row := range page.rows[1:24] {
		trimmed := strings.TrimRight(row, " ")
		if trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}
