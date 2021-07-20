package utils

import (
	"crypto/sha1"
	"crypto/sha256"
)

func Sha1Sum(s []byte) string {
	h := sha1.New()
	h.Write(s)
	return B2S(HexEncode(h.Sum(nil)))
}

func Sha256Sum(s []byte) string {
	h := sha256.New()
	h.Write(s)
	return B2S(HexEncode(h.Sum(nil)))
}
