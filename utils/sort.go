package utils

func Reverse(s []interface{}) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func ReversePair(s []interface{}, pair bool) {
	if !pair {
		Reverse(s)
		return
	}
	for i, j := 0, len(s)-2; i < j; i, j = i+2, j-2 {
		s[i], s[j] = s[j], s[i]
		s[i+1], s[j+1] = s[j+1], s[i+1]
	}
}

func ReverseBytes(s [][]byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}
