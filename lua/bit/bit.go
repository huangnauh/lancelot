package bit

import (
	"fmt"
	"math"

	lua "github.com/yuin/gopher-lua"
)

const (
	LibName = "bit"
)

func Preload(L *lua.LState) {
	L.PreloadModule("bit", Loader)
}

func Loader(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, api)
	L.Push(t)
	return 1
}

func OpenBit(L *lua.LState) int {
	bitmod := L.RegisterModule(LibName, api)
	L.Push(bitmod)
	return 1
}

var api = map[string]lua.LGFunction{
	"tobit":   apiToBit,
	"tohex":   apiToHex,
	"band":    apiBand,
	"bnot":    apiBnot,
	"bor":     apiBor,
	"bxor":    apiBxor,
	"lshift":  apiLshift,
	"rshift":  apiRshift,
	"arshift": apiArshift,
	"rol":     apiRol,
	"ror":     apiRor,
	"bswap":   apiBswap,
}

func CheckUnsignedNumber(L *lua.LState, n int) uint32 {
	v := L.Get(n)
	if lv, ok := v.(lua.LNumber); ok {
		const supUnsigned = float64(^uint32(0)) + 1
		return uint32(float64(lv) - math.Floor(float64(lv)/supUnsigned)*supUnsigned)
	}
	L.TypeError(n, lua.LTNumber)
	return 0
}

func trim(x uint32) uint32 { return x & math.MaxUint32 }
func mask(n uint32) uint32 { return ^(math.MaxUint32 << n) }

func apiToBit(L *lua.LState) int {
	v := CheckUnsignedNumber(L, 1)
	L.Push(lua.LNumber(v))
	return 1
}

func apiBnot(L *lua.LState) int {
	v := CheckUnsignedNumber(L, 1)
	L.Push(lua.LNumber(^v))
	return 1
}

func apiBswap(L *lua.LState) int {
	v := CheckUnsignedNumber(L, 1)
	L.Push(lua.LNumber(v<<24 | v<<8&0xff0000 | v>>8&0xff00 | v>>24))
	return 1
}

func apiRol(L *lua.LState) int {
	v := CheckUnsignedNumber(L, 1)
	n := L.CheckInt(2)
	if n &= 31; n != 0 {
		L.Push(lua.LNumber((v << n) | (v >> (32 - n))))
	} else {
		L.Push(lua.LNumber(v))
	}
	return 1
}

func apiRor(L *lua.LState) int {
	v := CheckUnsignedNumber(L, 1)
	n := L.CheckInt(2)
	if n &= 31; n != 0 {
		L.Push(lua.LNumber((v >> n) | (v << (32 - n))))
	} else {
		L.Push(lua.LNumber(v))
	}
	return 1
}

func apiLshift(L *lua.LState) int {
	if L.GetTop() < 2 {
		L.ArgError(1, "not enough arguments")
	}
	v := CheckUnsignedNumber(L, 1)
	n := L.CheckInt(2)
	if n >= 0 {
		L.Push(lua.LNumber(v << n))
	} else {
		L.Push(lua.LNumber(v >> -n))
	}
	return 1
}

func apiRshift(L *lua.LState) int {
	if L.GetTop() < 2 {
		L.ArgError(1, "not enough arguments")
	}
	v := CheckUnsignedNumber(L, 1)
	n := L.CheckInt(2)
	if n >= 0 {
		L.Push(lua.LNumber(v >> n))
	} else {
		L.Push(lua.LNumber(v << -n))
	}
	return 1
}

func apiArshift(L *lua.LState) int {
	if L.GetTop() < 2 {
		L.ArgError(1, "not enough arguments")
	}
	v := CheckUnsignedNumber(L, 1)
	n := L.CheckInt(2)
	if v>>31 == 0 || n < 0 {
		if n >= 0 {
			L.Push(lua.LNumber(v >> n))
		} else {
			L.Push(lua.LNumber(v << -n))
		}
	} else {
		if n >= 32 {
			L.Push(lua.LNumber(math.MaxUint32))
		} else {
			L.Push(lua.LNumber(v>>n | ^(math.MaxUint32 >> n)))
		}
	}
	return 1
}

func apiBand(L *lua.LState) int {
	num := L.GetTop()
	if num < 1 {
		L.ArgError(1, "not enough arguments")
	}
	val := uint32(math.MaxUint32)
	for i := 1; i <= num; i++ {
		v := CheckUnsignedNumber(L, i)
		val &= v
	}
	L.Push(lua.LNumber(val))
	return 1
}

func apiBor(L *lua.LState) int {
	num := L.GetTop()
	if num < 1 {
		L.ArgError(1, "not enough arguments")
	}
	val := uint32(0)
	for i := 1; i <= num; i++ {
		v := CheckUnsignedNumber(L, i)
		val |= v
	}
	L.Push(lua.LNumber(val))
	return 1
}

func apiBxor(L *lua.LState) int {
	num := L.GetTop()
	if num < 1 {
		L.ArgError(1, "not enough arguments")
	}
	val := uint32(0)
	for i := 1; i <= num; i++ {
		v := CheckUnsignedNumber(L, i)
		val ^= v
	}
	L.Push(lua.LNumber(val))
	return 1
}

func apiToHex(L *lua.LState) int {
	v := CheckUnsignedNumber(L, 1)
	num := 8
	flag := "x"
	if L.GetTop() >= 2 {
		n := L.CheckInt(2)
		if n < 0 {
			flag = "X"
			num = -n
		} else {
			num = n
		}

		if num > 8 {
			num = 8
		}
	}
	L.Push(lua.LString(fmt.Sprintf(fmt.Sprintf("%%08%s", flag), v)[8-num:]))
	return 1
}
