package utils

import (
	"encoding/binary"
	"fmt"
	"math"
)

type Number struct {
	IsInt   bool
	Uint64  uint64
	Float64 float64
}

func (n *Number) String() string {
	if n.IsInt {
		return fmt.Sprintf("%d", n.Uint64)
	}
	return fmt.Sprintf("%f", n.Float64)
}

func (n *Number) Decode(v []byte) {
	if n.IsInt {
		n.Uint64 = binary.BigEndian.Uint64(v)
	} else {
		n.Float64 = DecodeFloat(v)
	}
}

func (n Number) Encode(v []byte) {
	if n.IsInt {
		binary.BigEndian.PutUint64(v, n.Uint64)
	} else {
		EncodeFloatBytes(n.Float64, v)
	}
}

func (n *Number) Add(m Number) {
	if n.IsInt {
		n.Uint64 += m.Uint64
	} else {
		n.Float64 += m.Float64
	}
}

func (n Number) Compare(m Number) int {
	if m.IsInt {
		if n.Uint64 < m.Uint64 {
			return -1
		} else if n.Uint64 > m.Uint64 {
			return 1
		}
	} else {
		if n.Float64 < m.Float64 {
			return -1
		} else if n.Float64 > m.Float64 {
			return 1
		}
	}
	return 0
}

func (n Number) IsNan() bool {
	if !n.IsInt {
		return math.IsNaN(n.Float64)
	}
	return false
}
