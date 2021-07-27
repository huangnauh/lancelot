package utils

import (
	"encoding/hex"
	"reflect"
	"unsafe"
)

func NextKey(key []byte) []byte {
	buf := make([]byte, len(key)+1)
	copy(buf, key)
	buf[len(key)] = 0x00
	return buf
}

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

// b2s converts byte slice to a string without memory allocation.
// See https://groups.google.com/forum/#!msg/Golang-Nuts/ENgbUzYvCuU/90yGx7GUAgAJ .
//
// Note it may break if string and/or slice header will change
// in the future go versions.
func B2S(b []byte) string {
	/* #nosec G103 */
	return *(*string)(unsafe.Pointer(&b))
}

// s2b converts string to a byte slice without memory allocation.
//
// Note it may break if string and/or slice header will change
// in the future go versions.
func S2B(s string) (b []byte) {
	/* #nosec G103 */
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	/* #nosec G103 */
	sh := *(*reflect.StringHeader)(unsafe.Pointer(&s))
	bh.Data = sh.Data
	bh.Len = sh.Len
	bh.Cap = sh.Len
	return b
}

func HexEncode(src []byte) []byte {
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst
}

func HexDecode(src []byte, dst []byte) error {
	_, err := hex.Decode(dst, src)
	return err
}

func GetHexDecode(src []byte) ([]byte, error) {
	dst := make([]byte, hex.DecodedLen(len(src)))
	_, err := hex.Decode(dst, src)
	return dst, err
}
