package main

import (
	"fmt"

	lua "github.com/yuin/gopher-lua"
)

const (
	LUA_STR = `function fib(n)
	if n < 2 then
		return 1
	end
	return fib(n-1) + fib(n-2)
end
`
)

func main() {
	L := lua.NewState()
	defer L.Close()
	err := L.DoString("return a")
	if err != nil {
		panic(err)
	}
	ret := L.Get(-1)
	L.Pop(1)
	fmt.Println(ret)

	err = L.DoString(LUA_STR)
	if err != nil {
		panic(err)
	}
	err = L.CallByParam(lua.P{
		Fn:      L.GetGlobal("fib"),
		NRet:    1,
		Protect: true,
	}, lua.LNumber(10))
	if err != nil {
		panic(err)
	}
	ret = L.Get(-1)
	L.Pop(1)
	res, ok := ret.(lua.LNumber)
	if ok {
		fmt.Println(res)
	} else {
		panic(ret)
	}
}
