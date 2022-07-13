package utils

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

var (
	ErrPrecision = errors.New("precision lost")
)

func EncodeFloat(f float64) []byte {
	vi := math.Float64bits(f)
	if vi>>63 != 0 {
		vi = ^vi
	} else {
		vi |= (1 << 63)
	}
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, vi)
	return k
}

func EncodeFloatBytes(f float64, k []byte) {
	vi := math.Float64bits(f)
	if vi>>63 != 0 {
		vi = ^vi
	} else {
		vi |= (1 << 63)
	}
	binary.BigEndian.PutUint64(k, vi)
}

func DecodeFloat(k []byte) float64 {
	vi := binary.BigEndian.Uint64(k)
	if vi>>63 != 0 {
		vi ^= (1 << 63)
	} else {
		vi = ^vi
	}
	return math.Float64frombits(vi)
}

func MiddleFloat(left, right float64) (float64, error) {
	m := (left + right) / 2
	if m == left || m == right {
		return 0, ErrPrecision
	}
	bs := EncodeFloat(m)
	if bytes.Equal(bs, EncodeFloat(left)) {
		return 0, ErrPrecision
	}
	if bytes.Equal(bs, EncodeFloat(right)) {
		return 0, ErrPrecision
	}
	return m, nil
}

func ValidIncrementFloat(v, i float64) bool {
	if v >= 0 && math.MaxFloat64-v < i {
		return false
	}
	if v < 0 && -math.MaxFloat64-v > i {
		return false
	}
	return true
}

func ValidMultiFloat(v, i float64) bool {
	if v < 0 {
		v = -v
	}
	if i < 0 {
		i = -i
	}
	if (v >= 1) && math.MaxFloat64/v < i {
		return false
	}
	return true
}
