package cmsgpack

import (
	"errors"

	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
	lua "github.com/yuin/gopher-lua"
)

var (
	errNested      = errors.New("cannot encode recursively nested tables to msgpack")
	errInvalidKeys = errors.New("cannot encode mixed or invalid key types")
)

type invalidTypeError lua.LValueType

func (i invalidTypeError) Error() string {
	return `cannot encode ` + lua.LValueType(i).String() + ` to msgpack`
}

func Preload(L *lua.LState) {
	L.PreloadModule("msgpack", Loader)
}

func Loader(L *lua.LState) int {
	t := L.NewTable()
	L.SetFuncs(t, api)
	L.Push(t)
	return 1
}

var api = map[string]lua.LGFunction{
	"pack":   apiPack,
	"unpack": apiPack,
}

func apiPack(L *lua.LState) int {
	value := L.CheckAny(1)
	data, err := Encode(value)
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	L.Push(lua.LString(string(data)))
	return 1
}

type msgpackValue struct {
	lua.LValue
	visited map[*lua.LTable]bool
}

func (m msgpackValue) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.UseCompactFloats(true)
	logrus.Debugf("msgpack: encoding %v", m.LValue)
	switch v := m.LValue.(type) {
	case lua.LBool:
		return enc.EncodeBool(bool(v))
	case lua.LNumber:
		return enc.EncodeFloat64(float64(v))
	case *lua.LNilType:
		return enc.EncodeNil()
	case lua.LString:
		return enc.EncodeString(string(v))
	case *lua.LTable:
		if m.visited[v] {
			return errNested
		}
		m.visited[v] = true

		arr := make([]msgpackValue, 0)
		obj := make(map[string]msgpackValue)
		var err error
		v.ForEach(func(lk, lv lua.LValue) {
			switch lk.Type() {
			case lua.LTNumber:
				arr = append(arr, msgpackValue{lv, m.visited})
			case lua.LTString:
				obj[lk.String()] = msgpackValue{lv, m.visited}
			default:
				err = errInvalidKeys
				return
			}
		})
		if err != nil {
			return err
		}

		if len(obj) > 0 {
			err = enc.EncodeMapLen(len(obj) + len(arr))
			if err != nil {
				return err
			}
			for i, v := range arr {
				err = enc.EncodeInt(int64(i + 1))
				if err != nil {
					return err
				}
				err = v.EncodeMsgpack(enc)
				if err != nil {
					return err
				}
			}
			for mk, mv := range obj {
				err = enc.EncodeString(mk)
				if err != nil {
					return err
				}
				err = mv.EncodeMsgpack(enc)
				if err != nil {
					return err
				}
			}
			return nil
		} else {
			err = enc.EncodeArrayLen(len(arr))
			if err != nil {
				return err
			}
			for _, v := range arr {
				err = v.EncodeMsgpack(enc)
				if err != nil {
					return err
				}
			}
			return nil
		}
	default:
		return invalidTypeError(m.LValue.Type())
	}
}

func Encode(value lua.LValue) ([]byte, error) {
	return msgpack.Marshal(msgpackValue{
		LValue:  value,
		visited: make(map[*lua.LTable]bool),
	})
}
