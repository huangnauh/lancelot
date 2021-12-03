// Package command https://redis.io/commands#string
package command

import (
	"math"
	"strconv"
	"strings"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

type CheckType int
type StringFunc func(o *Object, args [][]byte) (interface{}, bool, error)

const (
	//EX seconds
	EX = "ex"
	//PX milliseconds
	PX = "px"
	//EXAT timestamp
	EXAT = "exat"
	//PXAT milliseconds-timestamp
	PXAT = "pxat"
	//KEEPTTL
	KEEPTTL = "keepttl"
	//PERSIST
	PERSIST = "persist"
	//NX
	NX = "nx"
	//XX
	XX = "xx"
	//CH
	CH = "ch"
	// LT
	LT = "lt"
	// GT
	GT = "gt"
	//GET
	GET = "get"
	//INCR
	INCR = "incr"

	NoCheck CheckType = 0x00
	// Only set the key if it does not already exist.
	CheckNotExist CheckType = 0x01
	// Only set the key if it already exist.
	CheckExist CheckType = 0x02
	CheckLT    CheckType = 0x04
	CheckGT    CheckType = 0x08
)

func (c *Command) getString(txn *store.Txn, arg []byte) ([]byte, error) {
	object := NewObject(txn.UserId, txn.DBId, StringType, arg)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		return object.Value, nil
	}
}

// (string) GET key
func (c *Command) GetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(GET_COMMAND)
	}
	value, err := c.getString(txn, args[0])
	if err != nil {
		return txn.SetError(err)
	}
	if value == nil {
		return nil
	}
	return value
}

// (string) MGET key [key ...]
func (c *Command) MGetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(MGET_COMMAND)
	}

	ret := make([]interface{}, len(args))
	for i := 0; i < len(args); i++ {
		value, err := c.getString(txn, args[i])
		if err == xerror.WrongTypeErr {
			ret[i] = nil
		} else if err != nil {
			return txn.SetError(err)
		}
		if value == nil {
			ret[i] = nil
		} else {
			ret[i] = value
		}
	}
	return ret
}

// (string) GETDEL key
func (c *Command) GetDelHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(GETDEL_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, StringType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	} else {
		value := object.Value
		err = DeleteKey(txn, key, object, txn.Now, MinusCount)
		if err != nil {
			return txn.SetError(err)
		}
		return value
	}
}

// (string) STRLEN key
func (c *Command) StrLenHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(STRLEN_COMMAND)
	}
	value, err := c.getString(txn, args[0])
	if err != nil {
		return txn.SetError(err)
	}
	return SimpleInt(int64(len(value)))
}

// (string) APPEND key value
func (c *Command) AppendHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(APPEND_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, StringType, args[0])
	key := object.GetKeyBytes()
	var create ChangeType
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		create = PlusCount
		object.Value = args[1]
	} else if err != nil {
		return txn.SetError(err)
	} else {
		object.Value = append(object.Value, args[1]...)
	}
	object.Timestamp = txn.Timestamp
	err = setTxnObject(txn, key, object, create)
	if err != nil {
		return txn.SetError(err)
	}
	return SimpleInt(int64(len(object.Value)))
}

type ExpireOption struct {
	KeepTTL bool
	Get     bool
	Expire  int64
	Check   CheckType
	Persist bool
}

func checkExpireOption(cmd string, args [][]byte, isSet bool, nowTime time.Time) (*ExpireOption, error) {
	opt := &ExpireOption{}
	for i := 0; i < len(args); i++ {
		str := strings.ToLower(utils.B2S(args[i]))
		intFlag := false
		var unitDuration time.Duration
		var unitInt64 int64
		switch str {
		case EX:
			if (unitDuration != 0 && unitDuration != time.Second) || opt.KeepTTL || opt.Persist {
				return nil, xerror.ErrSyntax
			}
			intFlag = true
			unitDuration = time.Second
		case PX:
			if (unitDuration != 0 && unitDuration != time.Millisecond) || opt.KeepTTL || opt.Persist {
				return nil, xerror.ErrSyntax
			}
			intFlag = true
			unitDuration = time.Millisecond
		case EXAT:
			if (unitInt64 != 0 && unitInt64 != 1000) || opt.KeepTTL || opt.Persist {
				return nil, xerror.ErrSyntax
			}
			intFlag = true
			unitInt64 = 1000
		case PXAT:
			if (unitInt64 != 0 && unitInt64 != 1) || opt.KeepTTL || opt.Persist {
				return nil, xerror.ErrSyntax
			}
			intFlag = true
			unitInt64 = 1
		case KEEPTTL:
			if !isSet || unitDuration > 0 || unitInt64 > 0 {
				return nil, xerror.ErrSyntax
			}
			opt.KeepTTL = true
		case PERSIST:
			if isSet || unitDuration > 0 || unitInt64 > 0 {
				return nil, xerror.ErrSyntax
			}
			opt.Persist = true
		case NX:
			if !isSet {
				return nil, xerror.ErrSyntax
			}
			opt.Check = CheckNotExist
		case XX:
			if !isSet {
				return nil, xerror.ErrSyntax
			}
			opt.Check = CheckExist
		case GET:
			if !isSet {
				return nil, xerror.ErrSyntax
			}
			opt.Get = true
		default:
			return nil, xerror.ErrSyntax
		}

		if intFlag {
			if i+1 == len(args) {
				return nil, xerror.WrongArgsError(cmd)
			}
			intArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return nil, xerror.ErrNotInteger
			}

			if intArg <= 0 {
				return nil, xerror.InvalidExpireError(cmd)
			}

			if unitDuration > 0 {
				if math.MaxInt64/int64(unitDuration) <= int64(intArg) {
					return nil, xerror.InvalidExpireError(cmd)
				}
				opt.Expire = nowTime.Add(time.Duration(intArg)*unitDuration).UnixNano() / int64(time.Millisecond)
			} else if unitInt64 > 0 {
				if math.MaxInt64/unitInt64/1000 <= int64(intArg) {
					return nil, xerror.InvalidExpireError(cmd)
				}
				opt.Expire = intArg * unitInt64
			}
			i++
		}
	}
	return opt, nil
}

func (c *Command) checkSetOption(txn *store.Txn, cmd string, key []byte, args [][]byte) (*Object, *ExpireOption, error) {
	nowTime := txn.NowTime()
	opt, err := checkExpireOption(cmd, args, true, nowTime)
	if err != nil {
		return nil, nil, err
	}

	oldObject, err := c.checkExist(txn, cmd, key, opt.Check)
	if err == store.KeyNotFound {
		return nil, opt, nil
	} else if err != nil {
		return oldObject, opt, err
	}

	oldExpire := oldObject.TTL
	if opt.KeepTTL && oldExpire > 0 {
		opt.Expire = oldExpire
	}
	opt.KeepTTL = opt.Expire == oldExpire

	if oldExpire > 0 {
		if !opt.KeepTTL {
			ttlKey := oldObject.GetTTLKeyBytes()
			err := txn.Del(ttlKey)
			if err != nil {
				return oldObject, opt, err
			}
		}
	}

	return oldObject, opt, nil
}

func (c *Command) checkExist(txn *store.Txn, cmd string, key []byte, check CheckType) (*Object, error) {
	cmdHandler, ok := c.TxnHandle[cmd]
	if !ok {
		return nil, xerror.UnknownCommandError(cmd)
	}
	oldObject := NewObject(txn.UserId, txn.DBId, c.getObjectType(cmdHandler.Typo), key)
	objectKey := oldObject.GetKeyBytes()
	err := getTxnObject(txn, objectKey, oldObject, false)
	if err == store.KeyNotFound {
		if CheckExist == check {
			return oldObject, xerror.ErrCheckFailed
		}
	} else if err != nil {
		return oldObject, err
	} else {
		if CheckNotExist == check {
			return oldObject, xerror.ErrCheckFailed
		}
	}
	return oldObject, err
}

// (string) GETSET key value
func (c *Command) GetSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(GETSET_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, StringType, args[0])
	key := object.GetKeyBytes()
	var create ChangeType
	err := getTxnObject(txn, key, object, true)
	var ret interface{}
	if err == store.KeyNotFound {
		ret = nil
		create = PlusCount
	} else if err != nil {
		return txn.SetError(err)
	} else {
		ret = object.Value
		if object.TTL > 0 {
			err = txn.Del(object.GetTTLKeyBytes())
			if err != nil {
				return txn.SetError(err)
			}
		}
	}
	object.Value = args[1]
	object.Timestamp = txn.Timestamp
	err = setTxnObject(txn, key, object, create)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func intFunc(o *Object, args [][]byte, delta int64) (interface{}, bool, error) {
	var value int64
	if o.Value == nil {
		value = delta
	} else {
		intValue, err := strconv.ParseInt(utils.B2S(o.Value), 10, 64)
		if err != nil {
			return nil, false, xerror.ErrNotInteger
		}
		if delta == 0 {
			return intValue, false, nil
		}
		ok := utils.ValidIncrementInt(intValue, delta)
		if !ok {
			return nil, false, xerror.ErrOverflow
		}
		value = intValue + delta
	}
	o.Value = utils.S2B(strconv.FormatInt(value, 10))
	return redcon.SimpleInt(value), true, nil
}

func incrByFloat(o *Object, args [][]byte) (interface{}, bool, error) {
	str := strings.ToLower(utils.B2S(args[1]))
	if str == "+inf" || str == "-inf" {
		return nil, false, xerror.ErrFloatInfinity
	}
	delta, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return nil, false, xerror.ErrNotFloat
	}
	var value float64
	if o.Value == nil {
		value = delta
	} else {
		floatValue, err := strconv.ParseFloat(utils.B2S(o.Value), 64)
		if err != nil {
			return nil, false, xerror.ErrNotFloat
		}
		if delta == 0 {
			return floatValue, false, nil
		}
		ok := utils.ValidIncrementFloat(floatValue, delta)
		if !ok {
			return nil, false, xerror.ErrOverflow
		}
		value = floatValue + delta
	}
	o.Value = utils.S2B(strconv.FormatFloat(value, 'f', -1, 64))
	return value, true, nil
}

func decr(o *Object, args [][]byte) (interface{}, bool, error) {
	return intFunc(o, args, -1)
}

func incr(o *Object, args [][]byte) (interface{}, bool, error) {
	return intFunc(o, args, 1)
}

func decrBy(o *Object, args [][]byte) (interface{}, bool, error) {
	intValue, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return nil, false, xerror.ErrNotInteger
	}
	return intFunc(o, args, -intValue)
}

func incrBy(o *Object, args [][]byte) (interface{}, bool, error) {
	intValue, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return nil, false, xerror.ErrNotInteger
	}
	return intFunc(o, args, intValue)
}

func (c *Command) stringHandle(txn *store.Txn, args [][]byte, stringFunc StringFunc) interface{} {
	var create ChangeType
	object := NewObject(txn.UserId, txn.DBId, StringType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		create = PlusCount
	} else if err != nil {
		return txn.SetError(err)
	}

	value, changed, err := stringFunc(object, args)
	if err != nil {
		return txn.SetError(err)
	}
	if !changed {
		return value
	}
	object.Timestamp = txn.Timestamp
	err = setTxnObject(txn, key, object, create)
	if err != nil {
		return txn.SetError(err)
	}
	return value
}

// (string) DECRBY key decrement
func (c *Command) DecrByHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(DECRBY_COMMAND)
	}
	return c.stringHandle(txn, args, decrBy)
}

// (string) DECR key
func (c *Command) DecrHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(DECR_COMMAND)
	}
	return c.stringHandle(txn, args, decr)
}

// (string) INCR key
func (c *Command) IncrHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(INCR_COMMAND)
	}
	return c.stringHandle(txn, args, incr)
}

// (string) INCRBYFLOAT key increment
func (c *Command) IncrByFloatHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(INCRBYFLOAT_COMMAND)
	}
	return c.stringHandle(txn, args, incrByFloat)
}

// (string) INCRBY key increment
func (c *Command) IncrByHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(INCRBY_COMMAND)
	}
	return c.stringHandle(txn, args, incrBy)
}

func (c *Command) setString(txn *store.Txn, argKey, argValue []byte, expire int64, check CheckType) error {
	var create ChangeType
	object := NewObject(txn.UserId, txn.DBId, StringType, argKey)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		if check == CheckExist {
			return xerror.ErrCheckFailed
		}
		create = PlusCount
	} else if err != nil {
		return err
	} else {
		if check == CheckNotExist {
			return xerror.ErrCheckFailed
		}
		if object.TTL > 0 && object.TTL != expire {
			ttlKey := object.GetTTLKeyBytes()
			err := txn.Del(ttlKey)
			if err != nil {
				return err
			}
		}
	}

	object.Value = argValue
	object.Timestamp = txn.Timestamp
	if expire > 0 && object.TTL != expire {
		object.TTL = expire
		ttlKey := object.GetTTLKeyBytes()
		err := txn.Put(ttlKey, []byte{1})
		if err != nil {
			return err
		}
	}
	err = setTxnObject(txn, key, object, create)
	if err != nil {
		return err
	}
	return nil
}

// (string) SETNX key value
func (c *Command) SetNXHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(SETNX_COMMAND)
	}
	err := c.setString(txn, args[0], args[1], 0, CheckNotExist)
	if err == xerror.ErrCheckFailed {
		return SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	} else {
		return SimpleInt(1)
	}
}

// (string) MSETNX key value [key value ...]
func (c *Command) MSetNXHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 || len(args)%2 != 0 {
		return txn.SetWrongArgs(MSETNX_COMMAND)
	}
	exists := make(map[string]bool, len(args)/2)
	for i := len(args) - 2; i >= 0; i -= 2 {
		str := utils.B2S(args[i])
		if ok := exists[str]; ok {
			continue
		}
		exists[str] = true
		err := c.setString(txn, args[i], args[i+1], 0, CheckNotExist)
		if err == xerror.ErrCheckFailed {
			txn.Err = err
			return SimpleInt(0)
		} else if err != nil {
			return txn.SetError(err)
		}
	}
	return SimpleInt(1)
}

// (string) MSET key value [key value ...]
func (c *Command) MSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 || len(args)%2 != 0 {
		return txn.SetWrongArgs(MSET_COMMAND)
	}
	for i := 0; i < len(args); i += 2 {
		err := c.setString(txn, args[i], args[i+1], 0, NoCheck)
		if err != nil {
			return txn.SetError(err)
		}
	}
	return OK
}

// (string) SETRANGE key offset value
func (c *Command) SetRangeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(SETRANGE_COMMAND)
	}

	offset, err := strconv.Atoi(utils.B2S(args[1]))
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	if offset < 0 {
		return txn.SetError(xerror.ErrOffset)
	}

	var create ChangeType
	object := NewObject(txn.UserId, txn.DBId, StringType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, true)
	var value []byte
	if err == store.KeyNotFound {
		if offset+len(args[2]) == 0 {
			return SimpleInt(0)
		}
		value = make([]byte, offset+len(args[2]))
		copy(value[offset:], args[2])
		create = PlusCount
	} else if err != nil {
		return txn.SetError(err)
	} else {
		if offset+len(args[2]) == 0 {
			return SimpleInt(int64(len(object.Value)))
		}
		if offset+len(args[2]) > len(object.Value) {
			value = make([]byte, offset+len(args[2]))
			if offset > len(object.Value) {
				copy(value[:len(object.Value)], object.Value)
			} else {
				copy(value[:offset], object.Value[:offset])
			}
			copy(value[offset:], args[2])
		} else {
			value = object.Value
			copy(value[offset:], args[2])
		}
	}
	object.Value = value
	object.Timestamp = txn.Timestamp
	err = setTxnObject(txn, key, object, create)
	if err != nil {
		return txn.SetError(err)
	}
	return SimpleInt(int64(len(value)))
}

// (string) PSETEX key seconds value
func (c *Command) PSetExHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(PSETEX_COMMAND)
	}
	return c.setExpireHandle(txn, args, 1)
}

// (string) SETEX key seconds value
func (c *Command) SetExHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(SETEX_COMMAND)
	}
	return c.setExpireHandle(txn, args, 1000)
}

func (c *Command) setExpireHandle(txn *store.Txn, args [][]byte, unit int64) interface{} {
	intArg, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	if intArg <= 0 {
		return txn.SetError(xerror.InvalidExpireError(SETEX_COMMAND))
	}

	expire := txn.Now + intArg*unit
	err = c.setString(txn, args[0], args[2], expire, NoCheck)
	if err != nil {
		return txn.SetError(err)
	}
	return OK
}

// (string) SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL] [NX|XX] [GET]
func (c *Command) SetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(SET_COMMAND)
	}

	var create ChangeType
	oldObject, setOption, err := c.checkSetOption(txn, SET_COMMAND, args[0], args[2:])
	if err == xerror.ErrCheckFailed {
		if setOption.Get {
			if oldObject != nil && oldObject.Value != nil {
				return oldObject.Value
			}
			return nil
		}
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	if oldObject == nil {
		create = PlusCount
	}

	object := NewObject(txn.UserId, txn.DBId, StringType, args[0])
	object.Value = args[1]
	object.TTL = setOption.Expire
	object.Timestamp = txn.Timestamp

	if !setOption.KeepTTL && setOption.Expire > 0 {
		ttlKey := object.GetTTLKeyBytes()
		err := txn.Put(ttlKey, []byte{1})
		if err != nil {
			return txn.SetError(err)
		}
	}

	key := object.GetKeyBytes()
	err = setTxnObject(txn, key, object, create)
	if err != nil {
		return txn.SetError(err)
	} else if setOption.Get {
		if oldObject != nil && oldObject.Value != nil {
			return oldObject.Value
		}
		return nil
	} else {
		return OK
	}
}

// (string) GETEX key [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|PERSIST]
func (c *Command) GetExHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(GETEX_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, StringType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	} else if len(args) == 0 {
		return object.Value
	}
	nowTime := txn.NowTime()
	opt, err := checkExpireOption(GETEX_COMMAND, args[1:], false, nowTime)
	if err != nil {
		return txn.SetError(err)
	}

	if opt.Persist {
		if object.TTL == 0 {
			return object.Value
		}
		object.TTL = 0
	} else if opt.Expire > 0 {
		if opt.Expire == object.TTL {
			return object.Value
		}
		object.TTL = opt.Expire
	}

	object.Timestamp = txn.Timestamp
	err = setTxnObject(txn, key, object, 0)
	if err != nil {
		return txn.SetError(err)
	}
	return object.Value
}

func getRange(value []byte, start, end int) ([]byte, int, int) {
	if start < 0 {
		start = len(value) + start
	}

	if end < 0 {
		end = len(value) + end
	}

	if end > len(value)-1 {
		end = len(value) - 1
	}

	if start > len(value) {
		return value[0:0], start, end
	}

	if end < start {
		return value[0:0], start, end
	}

	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	}

	return value[start : end+1], start, end
}

func (c *Command) checkAndGetRange(txn *store.Txn, k []byte, args [][]byte) ([]byte, int, int, error) {
	start, end := 0, -1
	var err error
	if len(args) > 0 {
		start, err = strconv.Atoi(utils.B2S(args[0]))
		if err != nil {
			return nil, 0, 0, xerror.ErrNotInteger
		}
	}
	if len(args) > 1 {
		end, err = strconv.Atoi(utils.B2S(args[1]))
		if err != nil {
			return nil, 0, 0, xerror.ErrNotInteger
		}
	}
	return c.getRange(txn, k, start, end)
}

func (c *Command) getRange(txn *store.Txn, k []byte, start, end int) ([]byte, int, int, error) {
	object := NewObject(txn.UserId, txn.DBId, StringType, k)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil, start, end, nil
	} else if err != nil {
		return nil, start, end, err
	}
	value := object.Value
	v, start, end := getRange(value, start, end)
	return v, start, end, nil
}

// (string) GETRANGE key start end
func (c *Command) GetRangeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(GETRANGE_COMMAND)
	}

	v, _, _, err := c.checkAndGetRange(txn, args[0], args[1:])
	if err != nil {
		return txn.SetError(err)
	}
	if len(v) == 0 {
		return EmptyString
	}
	return v
}
