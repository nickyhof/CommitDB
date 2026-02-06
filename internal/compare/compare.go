// Package compare provides shared value comparison utilities for query evaluation and indexing.
package compare

import (
	"strconv"
	"strings"
)

// Values compares two string values, trying numeric comparison first, then string.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
func Values(a, b string) int {
	// Try numeric comparison first
	aNum, aErr := strconv.ParseFloat(a, 64)
	bNum, bErr := strconv.ParseFloat(b, 64)

	if aErr == nil && bErr == nil {
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0
	}

	// Fall back to string comparison
	return strings.Compare(a, b)
}

// MatchLike evaluates SQL LIKE pattern matching.
// Supports % (any chars) and _ (single char) wildcards.
func MatchLike(value, pattern string) bool {
	// Convert SQL LIKE pattern to simple matching
	// % = any characters, _ = single character
	valueRunes := []rune(value)
	patternRunes := []rune(pattern)

	return matchLikeRunes(valueRunes, patternRunes, 0, 0)
}

// matchLikeRunes performs recursive LIKE pattern matching
func matchLikeRunes(value, pattern []rune, vi, pi int) bool {
	for pi < len(pattern) {
		if pattern[pi] == '%' {
			// Skip consecutive %
			for pi < len(pattern) && pattern[pi] == '%' {
				pi++
			}
			if pi >= len(pattern) {
				return true // % at end matches everything
			}
			// Try matching rest of pattern at each position
			for vi <= len(value) {
				if matchLikeRunes(value, pattern, vi, pi) {
					return true
				}
				vi++
			}
			return false
		} else if vi >= len(value) {
			return false // Pattern remains but value exhausted
		} else if pattern[pi] == '_' || pattern[pi] == value[vi] {
			vi++
			pi++
		} else {
			return false
		}
	}
	return vi >= len(value)
}
