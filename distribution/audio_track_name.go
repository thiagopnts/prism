package distribution

import (
	"fmt"
	"strconv"
	"strings"
)

// audioTrackName returns the MoQ catalog/track name for a zero-based audio track
// index. When language is non-empty it is appended as a "-<lang>" suffix, so
// index 0 with "eng" becomes "audio0-eng"; an empty language yields "audio0".
func audioTrackName(index int, language string) string {
	if language == "" {
		return fmt.Sprintf("audio%d", index)
	}
	return fmt.Sprintf("audio%d-%s", index, language)
}

// parseAudioTrackIndex extracts the zero-based track index from an audio track
// name of the form "audio<index>" or "audio<index>-<lang>". It returns
// (index, true) on success and (0, false) for names that are not audio tracks
// or are malformed — no digits after the prefix, or digits not immediately
// followed by end-of-string or a '-' separator.
func parseAudioTrackIndex(trackName string) (int, bool) {
	suffix, ok := strings.CutPrefix(trackName, "audio")
	if !ok || suffix == "" {
		return 0, false
	}
	end := 0
	for end < len(suffix) && suffix[end] >= '0' && suffix[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, false
	}
	if end < len(suffix) && suffix[end] != '-' {
		return 0, false
	}
	idx, err := strconv.Atoi(suffix[:end])
	if err != nil || idx < 0 {
		return 0, false
	}
	return idx, true
}
