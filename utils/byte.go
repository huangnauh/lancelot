package utils

func PrefixNext(prefix []byte) []byte {
	buf := make([]byte, len(prefix))
	copy(buf, prefix)
	var i int
	for i = len(prefix) - 1; i >= 0; i-- {
		buf[i]++
		if buf[i] != 0 {
			break
		}
	}
	if i == -1 {
		copy(buf, prefix)
		buf = append(buf, 0)
	}
	return buf
}
