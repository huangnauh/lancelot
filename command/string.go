// https://redis.io/commands#string
package command

import (
	"strconv"
	"strings"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

type CheckType int

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
	//GET
	GET = "get"

	NoCheck CheckType = 0
	// Only set the key if it already exist.
	CheckExist CheckType = 1
	// Only set the key if it does not already exist.
	CheckNotExist CheckType = 2
)

func getString(txn *store.Txn, arg []byte) ([]byte, error) {
	object := NewObject(txn.UserId, txn.DBId, KeyType, arg)
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
	value, err := getString(txn, args[0])
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
		value, err := getString(txn, args[i])
		if err != nil {
			return txn.SetError(err)
		}
		ret[i] = value
	}
	return ret
}

// (string) GETDEL key
func (c *Command) GetDelHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(GETDEL_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	} else {
		value := object.Value
		err = DeleteKey(txn, key, object, txn.Now)
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
	value, err := getString(txn, args[0])
	if err != nil {
		return txn.SetError(err)
	}
	return SimpleInt(len(value))
}

// (string) APPEND key value
func (c *Command) AppendHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(APPEND_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		object.Value = args[1]
	} else if err != nil {
		return txn.SetError(err)
	} else {
		object.Value = append(object.Value, args[1]...)
	}
	object.Timestamp = txn.Timestamp
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	}
	return SimpleInt(len(object.Value))
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
			if !isSet || opt.Get {
				return nil, xerror.ErrSyntax
			}
			opt.Check = CheckNotExist
		case XX:
			if !isSet {
				return nil, xerror.ErrSyntax
			}
			opt.Check = CheckExist
		case GET:
			if !isSet || opt.Check == CheckNotExist {
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
				opt.Expire = nowTime.Add(time.Duration(intArg)*unitDuration).UnixNano() / int64(time.Millisecond)
			} else if unitInt64 > 0 {
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
	if err != store.KeyNotFound && err != nil {
		return nil, nil, err
	}

	var oldExpire int64
	if oldObject != nil && oldObject.TTL > 0 {
		oldExpire = oldObject.TTL
	}

	if opt.KeepTTL && oldExpire > 0 {
		opt.Expire = oldExpire
	}
	opt.KeepTTL = opt.Expire == oldExpire

	if oldExpire > 0 {
		if !opt.KeepTTL {
			ttlKey := oldObject.GetTTLKeyBytes()
			err := txn.Del(ttlKey)
			if err != nil {
				return nil, nil, err
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
	oldObject := NewObject(txn.UserId, txn.DBId, cmdHandler.Type, key)
	objectKey := oldObject.GetKeyBytes()
	err := getTxnObject(txn, objectKey, oldObject, true)
	if err == store.KeyNotFound {
		if CheckExist == check {
			return nil, xerror.ErrCheckFailed
		}
	} else if err != nil {
		return nil, err
	} else {
		if CheckNotExist == check {
			return nil, xerror.ErrCheckFailed
		}
	}
	return oldObject, err
}

// (string) GETSET key value
func (c *Command) GetSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(GETSET_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	var ret interface{}
	if err == store.KeyNotFound {
		ret = nil
	} else if err != nil {
		return txn.SetError(err)
	} else {
		ret = object.Value
		err = DeleteKey(txn, key, object, txn.Now)
		if err != nil {
			return txn.SetError(err)
		}
	}
	object.Value = args[1]
	object.Timestamp = txn.Timestamp
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// (string) DECRBY key decrement
func (c *Command) DecrByHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(DECRBY_COMMAND)
	}
	intValue, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	return c.intHandle(txn, args[0], -intValue)
}

// (string) DECR key
func (c *Command) DecrHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(DECR_COMMAND)
	}
	return c.intHandle(txn, args[0], -1)
}

// (string) INCR key
func (c *Command) IncrHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(INCR_COMMAND)
	}
	return c.intHandle(txn, args[0], 1)
}

// (string) INCRBYFLOAT key increment
func (c *Command) IncrByFloatHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(INCRBYFLOAT_COMMAND)
	}
	delta, err := strconv.ParseFloat(utils.B2S(args[1]), 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotFloat)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, true)
	var value float64
	if err == store.KeyNotFound {
		value = delta
	} else if err != nil {
		return txn.SetError(err)
	} else {
		floatValue, err := strconv.ParseFloat(utils.B2S(object.Value), 64)
		if err != nil {
			return txn.SetError(xerror.ErrNotFloat)
		}
		value = floatValue + delta
	}
	object.Value = utils.S2B(strconv.FormatFloat(value, 'f', -1, 64))
	object.Timestamp = txn.Timestamp
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	} else {
		return value
	}
}

// (string) INCRBY key increment
func (c *Command) IncrByHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(INCRBY_COMMAND)
	}
	intValue, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	return c.intHandle(txn, args[0], intValue)
}

func (c *Command) intHandle(txn *store.Txn, arg []byte, delta int64) interface{} {
	object := NewObject(txn.UserId, txn.DBId, KeyType, arg)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	var value int64
	if err == store.KeyNotFound {
		value = delta
	} else if err != nil {
		return txn.SetError(err)
	} else {
		intValue, err := strconv.ParseInt(utils.B2S(object.Value), 10, 64)
		if err != nil {
			return txn.SetError(xerror.ErrNotInteger)
		}
		value = intValue + delta
	}
	object.Value = utils.S2B(strconv.FormatInt(value, 10))
	object.Timestamp = txn.Timestamp
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	} else {
		return SimpleInt(int(value))
	}
}

func setString(txn *store.Txn, argKey, argValue []byte, expire int64, check CheckType) error {
	object := NewObject(txn.UserId, txn.DBId, KeyType, argKey)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		if check == CheckExist {
			return xerror.ErrCheckFailed
		}
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
	object.TTL = expire
	object.Timestamp = txn.Timestamp
	err = txn.Put(key, ObjectEncode(object))
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
	err := setString(txn, args[0], args[1], 0, CheckNotExist)
	if err == xerror.ErrCheckFailed {
		return SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	} else {
		return SimpleInt(1)
	}
	// _, err := c.checkExist(txn, SETNX_COMMAND, args[0], CheckNotExist)
	// if err == xerror.ErrCheckFailed {
	// 	return SimpleInt(0)
	// } else if err != nil && err != store.KeyNotFound {
	// 	return txn.SetError(err)
	// }

	// object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	// object.Value = args[1]
	// object.Timestamp = txn.Timestamp
	// key := object.GetKeyBytes()
	// err = txn.Put(key, ObjectEncode(object))
	// if err != nil {
	// 	return txn.SetError(err)
	// } else {
	// 	return SimpleInt(1)
	// }
}

// (string) MSETNX key value [key value ...]
func (c *Command) MSetNXHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 || len(args)%2 != 0 {
		return txn.SetWrongArgs(MSETNX_COMMAND)
	}
	for i := 0; i < len(args); i += 2 {
		err := setString(txn, args[i], args[i+1], 0, CheckNotExist)
		if err == xerror.ErrCheckFailed {
			txn.Err = err
			return SimpleInt(0)
		} else if err != nil {
			return txn.SetError(err)
		}
	}
	return nil
}

// (string) MSET key value [key value ...]
func (c *Command) MSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 || len(args)%2 != 0 {
		return txn.SetWrongArgs(MSET_COMMAND)
	}
	for i := 0; i < len(args); i += 2 {
		err := setString(txn, args[i], args[i+1], 0, NoCheck)
		if err != nil {
			return txn.SetError(err)
		}
	}
	return SimpleInt(1)
}

// (string) SETEX key seconds value
func (c *Command) SetExHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(SETEX_COMMAND)
	}
	intArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	if intArg <= 0 {
		return txn.SetError(xerror.InvalidExpireError(SETEX_COMMAND))
	}

	expire := txn.Now + intArg*1000
	err = setString(txn, args[0], args[2], expire, NoCheck)
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

	oldObject, setOption, err := c.checkSetOption(txn, SET_COMMAND, args[0], args[2:])
	if err == xerror.ErrCheckFailed {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}

	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
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
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	} else if setOption.Get {
		if oldObject != nil && oldObject.Value != nil {
			return oldObject.Value
		} else {
			return nil
		}
	} else {
		return OK
	}
}

// (string) GETEX key [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|PERSIST]
func (c *Command) GetExHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(GETEX_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
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
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	}
	return object.Value
}

// (string) GETRANGE key start end
func (c *Command) GetRangeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(GETRANGE_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	start, err := strconv.Atoi(utils.B2S(args[1]))
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	end, err := strconv.Atoi(utils.B2S(args[2]))
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return EmptyString
	} else if err != nil {
		return txn.SetError(err)
	}
	value := object.Value
	if start < 0 {
		start = len(value) + start
		if start < 0 {
			start = 0
		}
	}

	if end < 0 {
		end = len(value) + end
		if end < 0 {
			end = 0
		}
	}

	if start > len(value) {
		return EmptyString
	}

	if end > len(value)-1 {
		end = len(value) - 1
	}

	return value[start : end+1]
}
