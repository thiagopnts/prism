package demux

import (
	"strings"

	"github.com/zsiec/prism/mpegts"
)

// descriptorTagISO639Language is the PMT descriptor tag (ISO/IEC 13818-1) that
// carries one or more 3-byte ISO 639-2 language codes for an elementary stream.
const descriptorTagISO639Language = 0x0A

// audioLanguageFromDescriptors returns a validated language label taken from the
// first ISO 639 language descriptor (tag 0x0A) found in an elementary stream's
// descriptors, or "" when none is present or the code fails validation. Only the
// first language entry's 3-byte code is used.
func audioLanguageFromDescriptors(descs []mpegts.PMTDescriptor) string {
	for _, d := range descs {
		if d.Tag == descriptorTagISO639Language && len(d.Data) >= 3 {
			return sanitizeLanguage(d.Data[:3])
		}
	}
	return ""
}

// sanitizeLanguage trims NUL and space padding from a raw language code and
// returns it unchanged (case preserved) only when the trimmed result is
// non-empty and consists solely of ASCII letters. Any other input (empty,
// interior spaces, digits, or non-ASCII bytes) yields "".
func sanitizeLanguage(raw []byte) string {
	s := strings.Trim(string(raw), "\x00 ")
	if s == "" {
		return ""
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			return ""
		}
	}
	return s
}
