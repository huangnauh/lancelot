package main

import (
	"fmt"
	"reflect"

	"github.com/tidwall/sjson"
	"github.com/valyala/fastjson"
	"github.com/vmihailenco/msgpack/v5"
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

const (
	json     = `{"name":{"first":"Janet","last":"Prichard"},"age"::47}`
	children = `["Sara","Alex","Jack"]`
)

func main4() {
	v := fastjson.MustParse(`{"foo":1,"bar":[2,3]}`)

	// Replace `foo` value with "xyz"
	v.Set("foo", fastjson.MustParse(`"xyz"`))
	// Add "newv":123
	v.Set("newv", fastjson.MustParse(`123`))
	fmt.Printf("%s\n", v)

	// Replace `bar.1` with {"x":"y"}
	v.Get("bar").Set("1", fastjson.MustParse(`{"x":"y"}`))
	fmt.Printf("%s\n", v)
	// Add `bar.3="qwe"
	v.Get("bar").Set("3", fastjson.MustParse(`"qwe"`))
	fmt.Printf("%s\n", v)
	fmt.Printf("%s\n", v.Get("bar", "1"))

}

type ExtTest struct {
	S string
}

func main() {

	j, _ := sjson.Set("", "name", json)
	json1, err := sjson.Delete(json, "aaaa")
	// json1, _ := sjson.Set("", "name", results[0].Value())
	// json1, _ = sjson.Set(json1, "age", results[1].Value())
	fmt.Println(j)
	fmt.Println(json1)
	fmt.Println(err)
	// fmt.Println(json1)

	v := ExtTest{S: "test"}
	body, err := msgpack.Marshal(&v)
	fmt.Printf("%v\n", body)
	fmt.Println(err)
	var v1 interface{}
	err = msgpack.Unmarshal(body, &v1)
	fmt.Println(err)

	fmt.Printf("%#v, %s", v1, reflect.TypeOf(v1))
}

func main1() {
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
