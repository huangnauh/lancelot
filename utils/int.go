package utils

import (
	"errors"
	"math"
	"strconv"
)

const signMask64 uint64 = 0x8000000000000000
const signMask16 uint16 = 0x8000

var (
	ErrInvalidInt = errors.New("invalid int")
)

func GetNonnegativeInt64(arg []byte) (int64, error) {
	offset, err := strconv.ParseInt(B2S(arg), 10, 64)
	if err != nil {
		return 0, ErrInvalidInt
	}
	if offset < 0 {
		return 0, ErrInvalidInt
	}
	return offset, nil
}

func GetPositiveInt(arg []byte) (int, error) {
	offset, err := strconv.Atoi(B2S(arg))
	if err != nil {
		return 0, err
	}
	if offset <= 0 {
		return offset, ErrInvalidInt
	}
	return offset, nil
}

func ValidIncrementInt(v, i int64) bool {
	if v >= 0 && math.MaxInt64-v < i {
		return false
	}
	if v < 0 && math.MinInt64-v > i {
		return false
	}
	return true
}

func EncodeInt64ToCmpUint(v int64) uint64 {
	return uint64(v) ^ signMask64
}

func DecodeCmpUintToInt64(u uint64) int64 {
	return int64(u ^ signMask64)
}

func EncodeInt16ToCmpUint(v int16) uint16 {
	return uint16(v) ^ signMask16
}

func DecodeCmpUintToInt16(u uint16) int16 {
	return int16(u ^ signMask16)
}
