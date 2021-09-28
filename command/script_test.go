package command

import (
	"reflect"
	"testing"

	lua "github.com/yuin/gopher-lua"
)

var scriptTests = [][]interface{}{
	{10, lua.LNumber(10)},
	{"aaa", lua.LString("aaa")},
	{[]byte("aaa"), lua.LString("aaa")},
	// {[][]byte{[]byte("aaa"), []byte("bbb")}, lua.LTable("aaa")},
}

var scriptTests1 = [][]interface{}{
	{SimpleInt(10), lua.LNumber(10)},
	{"aaa", lua.LString("aaa")},
}

func TestConvert(t *testing.T) {
	L := lua.NewState(lua.Options{
		CallStackSize:       lua.CallStackSize,
		RegistrySize:        lua.RegistrySize,
		RegistryMaxSize:     lua.RegistrySize * 2,
		IncludeGoStackTrace: true,
	})
	for _, tt := range scriptTests {
		a := covertToLua(L, tt[0])
		if !reflect.DeepEqual(tt[1], a) {
			t.Errorf("covertToLua(%v) = %v; want %v", tt[0], a, tt[1])
		}

	}
	for _, tt := range scriptTests1 {
		b := covertLuaValue(tt[1].(lua.LValue))
		if !reflect.DeepEqual(tt[0], b) {
			t.Errorf("covertLuaValue(%v) = %v; want %v", tt[1], b, tt[0])
		}
	}
}
