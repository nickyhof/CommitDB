package compare

import "testing"

func TestValuesNumeric(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"10", "9", 1},  // numeric: 10 > 9
		{"9", "25", -1}, // numeric: 9 < 25 (not string!)
		{"100", "99", 1},
		{"5", "5", 0},
		{"abc", "abd", -1}, // string fallback
		{"", "1", -1},
	}

	for _, tt := range tests {
		got := Values(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("Values(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestMatchLike(t *testing.T) {
	tests := []struct {
		value, pattern string
		want           bool
	}{
		{"hello", "%", true},
		{"hello", "hello", true},
		{"hello", "h%", true},
		{"hello", "%o", true},
		{"hello", "%ell%", true},
		{"hello", "h_llo", true},
		{"hello", "world", false},
	}

	for _, tt := range tests {
		got := MatchLike(tt.value, tt.pattern)
		if got != tt.want {
			t.Errorf("MatchLike(%q, %q) = %v, want %v", tt.value, tt.pattern, got, tt.want)
		}
	}
}
