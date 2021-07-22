package utils

import "strings"

func SplitDot(s string) []string {
	a := make([]string, 0)
	i := 0
	start := 0
	for i < len(s) {
		m := strings.Index(s[i:], ".")
		if m < 0 {
			break
		}

		i += m + 1
		if i < 2 {
			start = i
			continue
		}

		if s[i-2] == '\\' {
			continue
		}
		if s[start:i-1] != "" {
			a = append(a, s[start:i-1])
		}
		start = i
	}

	if s[start:] != "" {
		a = append(a, s[start:])
	}
	return a
}
