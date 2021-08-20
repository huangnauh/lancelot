package utils

import (
	"errors"
	"strconv"
)

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
