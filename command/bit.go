package command

import (
	"math/bits"
	"strconv"
	"strings"

	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

// (string) BITCOUNT key [start end]
func (c *Command) BitCountHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 3 {
		return txn.SetError(xerror.WrongArgsError(BITCOUNT_COMMAND))
	}

	v, _, _, err := c.checkAndGetRange(txn, args[0], args[1:])
	if err != nil {
		return txn.SetError(err)
	}

	if err != nil {
		return txn.SetError(err)
	}
	if len(v) == 0 {
		return SimpleInt(0)
	}
	return SimpleInt(int64(OneCount(v)))
}

// (string) BITPOS key bit [start [end]]
func (c *Command) BitPosHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 || len(args) > 4 {
		return txn.SetError(xerror.WrongArgsError(BITPOS_COMMAND))
	}

	bit, err := utils.GetNonnegativeInt64(args[1])
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	if bit != 0 && bit != 1 {
		return txn.SetError(xerror.ErrNotInteger)
	}

	var skipValue uint8
	if bit == 0 {
		skipValue = 0xFF
	}

	v, start, _, err := c.checkAndGetRange(txn, args[0], args[2:])
	if err != nil {
		return txn.SetError(err)
	}
	if v == nil {
		if bit == 1 {
			return SimpleInt(-1)
		} else {
			return SimpleInt(0)
		}
	}

	for ik, iv := range v {
		vv := uint8(iv)
		if vv == skipValue {
			continue
		}
		for i := 0; i < 8; i++ {
			isZero := vv&(1<<uint8(7-i)) == 0
			if (bit == 0 && isZero) || (bit == 1 && !isZero) {
				return SimpleInt(int64((start+ik)*8 + i))
			}
		}
	}
	return SimpleInt(-1)
}

func OneCount(v []byte) int {
	sum := 0
	for _, b := range v {
		sum += bits.OnesCount8(b)
	}
	return sum
}

// (string) GETBIT key offset
func (c *Command) GetBitHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetError(xerror.WrongArgsError(GETBIT_COMMAND))
	}

	offset, err := utils.GetNonnegativeInt64(args[1])
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	o := int(offset) >> 3
	v, _, _, err := c.getRange(txn, args[0], o, o)
	if err != nil {
		return txn.SetError(err)
	}
	if len(v) == 0 {
		return SimpleInt(0)
	}
	bit := 7 - offset&0x7
	return SimpleInt(int64(v[0]>>bit) & 1)
}

func AND(a, b byte) byte { return a & b }
func OR(a, b byte) byte  { return a | b }
func XOR(a, b byte) byte { return a ^ b }
func NOT(a, b byte) byte { return ^a }

var mapBitOp = map[string]func(a, b byte) byte{
	"and": AND,
	"or":  OR,
	"xor": XOR,
	"not": NOT,
}

func sliceBinOp(f func(a, b byte) byte, a, b []byte) []byte {
	maxl := len(a)
	if len(b) > maxl {
		maxl = len(b)
	}
	lA := make([]byte, maxl)
	copy(lA, a)
	lB := make([]byte, maxl)
	copy(lB, b)
	res := make([]byte, maxl)
	for i := range res {
		res[i] = f(lA[i], lB[i])
	}
	return res
}

// (string) BITOP operation destkey key [key ...]
func (c *Command) BitOpHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetError(xerror.WrongArgsError(BITOP_COMMAND))
	}
	str := strings.ToLower(utils.B2S(args[0]))
	if _, ok := mapBitOp[str]; !ok {
		return txn.SetError(xerror.ErrSyntax)
	}

	destObject := NewObject(txn.UserId, txn.DBId, StringType, args[1])
	destKey := destObject.GetKeyBytes()
	err := getTxnObject(txn, destKey, destObject, true)
	if err == store.KeyNotFound {
	} else if err != nil {
		return txn.SetError(err)
	}

	if str == "not" {
		object := NewObject(txn.UserId, txn.DBId, StringType, args[2])
		key := object.GetKeyBytes()
		err := getTxnObject(txn, key, object, false)
		if err == store.KeyNotFound {
			return SimpleInt(0)
		} else if err != nil {
			return txn.SetError(err)
		}
		value := object.Value
		for i := range value {
			value[i] = ^value[i]
		}
		destObject.Value = value
	} else {
		var ret []byte
		for i := 2; i < len(args); i++ {
			object := NewObject(txn.UserId, txn.DBId, StringType, args[i])
			key := object.GetKeyBytes()
			err := getTxnObject(txn, key, object, false)
			if err == store.KeyNotFound {
			} else if err != nil {
				return txn.SetError(err)
			} else if i == 2 {
				ret = object.Value
			} else {
				ret = sliceBinOp(mapBitOp[str], ret, object.Value)
			}
		}
		if len(ret) == 0 {
			return SimpleInt(0)
		}
		destObject.Value = ret
	}

	err = txn.Put(destKey, ObjectEncode(destObject))
	if err != nil {
		return txn.SetError(err)
	}

	return SimpleInt(int64(len(destObject.Value)))
}

// (string) SETBIT key offset value
func (c *Command) SetBitHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetError(xerror.WrongArgsError(SETBIT_COMMAND))
	}

	offset, err := utils.GetNonnegativeInt64(args[1])
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}

	bitChange, err := utils.GetNonnegativeInt64(args[2])
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	if bitChange != 0 && bitChange != 1 {
		return txn.SetError(xerror.ErrNotInteger)
	}

	o := int(offset) >> 3
	var value []byte
	var v byte
	object := NewObject(txn.UserId, txn.DBId, StringType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		value = make([]byte, o+1)
	} else if err != nil {
		return txn.SetError(err)
	} else {
		value = object.Value
		if o >= len(value) {
			value = append(value, make([]byte, o-len(value)+1)...)
		}
	}
	v = value[o]
	bit := 7 - offset&0x7
	origin := int64(v>>bit) & 1
	if origin == bitChange {
		return SimpleInt(origin)
	}
	value[o] = v & ^(1<<bit) | (uint8(bitChange) << bit)
	object.Value = value
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	}
	return SimpleInt(origin)
}

type BitInt struct {
	Signed bool
	Length int
}

func getBitInt(arg []byte) (b BitInt, err error) {
	b = BitInt{}
	if len(arg) <= 1 {
		return b, xerror.ErrSyntax
	}

	b.Length, err = utils.GetPositiveInt(arg[1:])
	if err != nil {
		return b, xerror.ErrBitFieldType
	}
	if arg[0] == 'i' {
		b.Signed = true
		if b.Length > 64 {
			return b, xerror.ErrBitFieldType
		}
	} else if arg[0] == 'u' {
		b.Signed = false
		if b.Length >= 64 {
			return b, xerror.ErrBitFieldType
		}
	} else {
		return b, xerror.ErrBitFieldType
	}
	return b, nil
}

type Method byte

const (
	BitGetMethod    Method = 'g'
	BitSetMethod    Method = 's'
	BitIncrbyMethod Method = 'i'

	WRAP = "warp"
	SAT  = "sat"
	FAIL = "fail"
)

type BitField struct {
	Method   Method
	Type     BitInt
	Offset   int64
	Value    int64
	Overflow string
}

// BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]
func (c *Command) BitFieldHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetError(xerror.WrongArgsError(BITFIELD_COMMAND))
	}

	overflow := WRAP
	bfs := make([]BitField, 0)
	for i := 1; i < len(args); i++ {
		str := strings.ToLower(utils.B2S(args[i]))
		bf := BitField{}
		if str == "get" {
			if len(args) < i+3 {
				return txn.SetError(xerror.ErrSyntax)
			}
			bf = BitField{
				Method: BitGetMethod,
			}
		} else if str == "set" {
			if len(args) < i+4 {
				return txn.SetError(xerror.ErrSyntax)
			}
			value, err := strconv.ParseInt(utils.B2S(args[i+3]), 10, 64)
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
			bf = BitField{
				Method: BitSetMethod,
				Value:  value,
			}
		} else if str == "incrby" {
			if len(args) < i+4 {
				return txn.SetError(xerror.ErrSyntax)
			}
			increment, err := strconv.ParseInt(utils.B2S(args[i+3]), 10, 64)
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
			bf = BitField{
				Method:   BitIncrbyMethod,
				Value:    increment,
				Overflow: overflow,
			}
		} else if str == "overflow" {
			if len(args) < i+2 {
				return txn.SetError(xerror.ErrSyntax)
			}
			str := strings.ToLower(utils.B2S(args[i+1]))
			if str == WRAP || str == SAT || str == FAIL {
				overflow = str
			} else {
				return txn.SetError(xerror.ErrSyntax)
			}
		} else {
			return txn.SetError(xerror.ErrSyntax)
		}

		bitInt, err := getBitInt(args[i+1])
		if err != nil {
			return txn.SetError(err)
		}
		bf.Type = bitInt
		offset, err := utils.GetNonnegativeInt64(args[i+2])
		if err != nil {
			return txn.SetError(xerror.ErrNotInteger)
		}
		bf.Offset = offset
		bfs = append(bfs, bf)
	}
	if len(bfs) == 0 {
		return []struct{}{}
	}

	// TODO: BITFIELD
	// object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	// key := object.GetKeyBytes()
	// err := getTxnObject(txn, key, object, true)
	// var value []byte
	// if err == store.KeyNotFound {
	// } else if err != nil {
	// 	return txn.SetError(err)
	// } else {
	// 	value = object.Value
	// }
	// ret := make([]interface{}, len(bfs))
	// for i, bf := range bfs {
	// 	startBit := bf.Type.Length * int(bf.Offset)
	// 	start := startBit >> 3
	// 	if start >= len(value) {
	// 		ret[i] = 0
	// 		continue
	// 	}

	// 	if bf.Method == BitGetMethod {
	// 		if bf.Type.Signed {
	// 			intv := int(value[start] >> (startBit & 0x7) & ^(1 << bf.Type.Length))
	// 			if intv > (1 << bf.Type.Length-1)-1 {
	// 			}
	// 		} else {
	// 			ret[i] = uint(value[start] >> (startBit & 0x7) & ^(1 << bf.Type.Length))
	// 		}
	// 		if value > (1 << bf.Type.Length)-1 {

	// 	}

	// }
	return nil
}
