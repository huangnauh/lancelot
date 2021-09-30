package bit

import (
	"testing"

	"github.com/stretchr/testify/assert"
	lua "github.com/yuin/gopher-lua"
)

var bitTests = [][]interface{}{
	{"return bit.tobit(10)", lua.LNumber(10)},
	{"return bit.tobit(0xffffffff)", lua.LNumber(0xffffffff)},
	{"return bit.tobit(0xffffffff + 1)", lua.LNumber(0)},
	{"return bit.tobit(2^40 + 1234)", lua.LNumber(1234)},
	{"return bit.tohex(0)", lua.LString("00000000")},
	{"return bit.tohex(0, 1)", lua.LString("0")},
	{"return bit.tohex(1)", lua.LString("00000001")},
	{"return bit.tohex(-1)", lua.LString("ffffffff")},
	{"return bit.tohex(0xffffffff)", lua.LString("ffffffff")},
	{"return bit.tohex(-1, -8)", lua.LString("FFFFFFFF")},
	{"return bit.tohex(0x21, 4)", lua.LString("0021")},
	{"return bit.tohex(0x87654321, 4)", lua.LString("4321")},
	{"return bit.band(0,0)", lua.LNumber(0)},
	{"return bit.band(0xffffffff,0xffffffff)", lua.LNumber(0xffffffff)},
	{"return bit.band(0xffffffff,0)", lua.LNumber(0)},
	{"return bit.band(0xa207f158,0x2e1054c9)", lua.LNumber(0x22005048)},
	{"return bit.band(0xa207f158)", lua.LNumber(0xa207f158)},
	{"return bit.band(0x2e1054c9,0x2e1054c9)", lua.LNumber(0x2e1054c9)},
	{"return bit.band(0x2e1054c9,0x2e1054c9, 0x2e1054c9)", lua.LNumber(0x2e1054c9)},
	{"return bit.bor(0,0)", lua.LNumber(0)},
	{"return bit.bor(0xffffffff,0)", lua.LNumber(0xffffffff)},
	{"return bit.bor(0xa207f158,0x2e1054c9)", lua.LNumber(0xae17f5d9)},
	{"return bit.bor(0x2e1054c9,0x2e1054c9)", lua.LNumber(0x2e1054c9)},
	{"return bit.bor(0x2e1054c9,0x2e1054c9, 0x2e1054c9)", lua.LNumber(0x2e1054c9)},
	{"return bit.bxor(0,0)", lua.LNumber(0)},
	{"return bit.bxor(0xffffffff,0)", lua.LNumber(0xffffffff)},
	{"return bit.bxor(0xa207f158,0x2e1054c9)", lua.LNumber(0x8c17a591)},
	{"return bit.bxor(0x2e1054c9,0x2e1054c9)", lua.LNumber(0)},
	{"return bit.bxor(0x2e1054c9,0x2e1054c9, 0x2e1054c9)", lua.LNumber(0x2e1054c9)},
	{"return bit.bnot(0)", lua.LNumber(0xffffffff)},
	{"return bit.bnot(1)", lua.LNumber(0xfffffffe)},
	{"return bit.bnot(0xffffffff)", lua.LNumber(0)},
	{"return bit.bnot(0x13579bdf)", lua.LNumber(0xeca86420)},
	{"return bit.band(0x13579bdf, bit.bnot(0x13579bdf))", lua.LNumber(0)},
	{"return bit.lshift(0x12345678, 4)", lua.LNumber(0x23456780)},
	{"return bit.lshift(0x12345678, 8)", lua.LNumber(0x34567800)},
	{"return bit.lshift(0x12345678, 32)", lua.LNumber(0)},
	{"return bit.rshift(0x12345678, 4)", lua.LNumber(0x01234567)},
	{"return bit.rshift(0x12345678, 8)", lua.LNumber(0x00123456)},
	{"return bit.rshift(0x12345678, 32)", lua.LNumber(0)},
	{"return bit.arshift(0, 0)", lua.LNumber(0)},
	{"return bit.arshift(0xffffffff, 0)", lua.LNumber(0xffffffff)},
	{"return bit.arshift(0xffffffff, 4)", lua.LNumber(0xffffffff)},
	{"return bit.arshift(0xffffffff, 32)", lua.LNumber(0xffffffff)},
	{"return bit.arshift(0x12345678, 1)", lua.LNumber(0x12345678 / 2)},
	{"return bit.arshift(0x12345678, -1)", lua.LNumber(0x12345678 * 2)},
	{"return bit.arshift(0x7fffffff, 31)", lua.LNumber(0)},
	{"return bit.arshift(0x7fffffff, 32)", lua.LNumber(0)},
	{"return bit.arshift(-1, 1)", lua.LNumber(0xffffffff)},
	{"return bit.rol(0x12345678, 12)", lua.LNumber(0x45678123)},
	{"return bit.ror(0x12345678, 12)", lua.LNumber(0x67812345)},
	{"return bit.bswap(0x12345678)", lua.LNumber(0x78563412)},
	{"return bit.bswap(0x78563412)", lua.LNumber(0x12345678)},
}

func TestConvert(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	L.Push(L.NewFunction(OpenBit))
	L.Push(lua.LString(LibName))
	L.Call(1, 0)
	for _, tt := range bitTests {
		err := L.DoString(tt[0].(string))
		assert.NoError(t, err, err)
		lv := L.Get(-1)
		assert.Equal(t, tt[1], lv, "test %s", tt[0])
	}
}
