package utils

import (
	"reflect"
	"testing"
)

type PrevKeyTest struct {
	key  []byte
	prev []byte
}

var prekeytests = []PrevKeyTest{
	{nil, nil},
	{make([]byte, 0), nil},
	{make([]byte, 10), nil},
	{[]byte{0x00, 0x00}, nil},
	{[]byte{0x1, 0x2}, []byte{0x1, 0x1, 0xff}},
	{[]byte{0x1, 0x0}, []byte{0x0, 0xff}},
	{[]byte{0x1, 0x0, 0x0, 0x0}, []byte{0x0, 0xff}},
	{[]byte{0xff, 0x0, 0x0, 0x0}, []byte{0xfe, 0xff}},
}

func TestPrevKey(t *testing.T) {
	for _, tt := range prekeytests {
		b := PrevKey(tt.key)
		if !reflect.DeepEqual(tt.prev, b) {
			t.Errorf("PrevKey(%q) = %v; want %v", tt.key, b, tt.prev)
		}
	}
}
