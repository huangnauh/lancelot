package utils

import (
	"crypto/sha1"
	"encoding/hex"
)

func Sha1Sum(s []byte) string {
	h := sha1.New()
	h.Write(s)
	return hex.EncodeToString(h.Sum(nil))
}
