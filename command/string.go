// https://redis.io/commands#string
package command

import (
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
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

	// Only set the key if it already exist.
	CheckExist CheckType = 1
	// Only set the key if it does not already exist.
	CheckNotExist CheckType = 2
)

// (string) GET key
func (c *Command) GetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(GET_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	startTs := txn.StartTS()
	now := oracle.ExtractPhysical(startTs)
	err := getTxnObject(txn, key, object, now, false)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	} else {
		return object.Value
	}
}

// (string) GETDEL key
func (c *Command) GetDelHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(GETDEL_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	startTs := txn.StartTS()
	now := oracle.ExtractPhysical(startTs)
	err := getTxnObject(txn, key, object, now, false)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	} else {
		value := object.Value
		err = DeleteKey(txn, key, object, now)
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
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	startTs := txn.StartTS()
	now := oracle.ExtractPhysical(startTs)
	err := getTxnObject(txn, key, object, now, false)
	if err == store.KeyNotFound {
		return SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	} else {
		return SimpleInt(len(object.Value))
	}
}

// (string) APPEND key value
func (c *Command) AppendHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(APPEND_COMMAND)
	}
	startTs := txn.StartTS()
	now := oracle.ExtractPhysical(startTs)
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, now, true)
	if err == store.KeyNotFound {
		object = NewObject(txn.UserId, txn.DBId, KeyType, args[0])
		object.Timestamp = startTs
		object.Value = args[1]
		err = txn.Put(key, ObjectEncode(object))
		if err != nil {
			return txn.SetError(err)
		}
		return SimpleInt(len(object.Value))
	} else if err != nil {
		return txn.SetError(err)
	} else {
		object.Timestamp = startTs
		object.Value = append(object.Value, args[1]...)
		err = txn.Put(key, ObjectEncode(object))
		if err != nil {
			return txn.SetError(err)
		}
		return SimpleInt(len(object.Value))
	}
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
				return nil, xerror.ErrInvalidExpire
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
	startTs := txn.StartTS()
	now := oracle.ExtractPhysical(startTs)
	nowTime := time.Unix(now/1e3, (now%1e3)*1e6)
	opt, err := checkExpireOption(cmd, args, true, nowTime)
	if err != nil {
		return nil, nil, err
	}

	oldObject, err := c.checkExist(txn, cmd, key, opt.Check, now)
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

func (c *Command) checkExist(txn *store.Txn, cmd string, key []byte, check CheckType, now int64) (*Object, error) {
	cmdHandler, ok := c.TxnHandle[cmd]
	if !ok {
		return nil, xerror.UnknownCommandError(cmd)
	}
	oldObject := NewObject(txn.UserId, txn.DBId, cmdHandler.Type, key)
	objectKey := oldObject.GetKeyBytes()
	err := getTxnObject(txn, objectKey, oldObject, now, true)
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

// (string) SETNX key value
func (c *Command) SetNXHandle(txn *store.Txn, args [][]byte) interface{} {
	return c.checkSetHandle(txn, args, CheckNotExist)
}

// (string) SETXX key value
func (c *Command) SetXXHandle(txn *store.Txn, args [][]byte) interface{} {
	return c.checkSetHandle(txn, args, CheckExist)
}

func (c *Command) checkSetHandle(txn *store.Txn, args [][]byte, check CheckType) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(SETNX_COMMAND)
	}
	startTs := txn.StartTS()
	now := oracle.ExtractPhysical(startTs)
	_, err := c.checkExist(txn, SETNX_COMMAND, args[0], CheckNotExist, now)
	if err == xerror.ErrCheckFailed {
		return SimpleInt(0)
	} else if err != nil && err != store.KeyNotFound {
		return txn.SetError(err)
	}

	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	object.Value = args[1]
	object.Timestamp = startTs
	key := object.GetKeyBytes()
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	} else {
		return SimpleInt(1)
	}
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
	startTs := txn.StartTS()
	now := oracle.ExtractPhysical(startTs)
	err := getTxnObject(txn, key, object, now, true)
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
	object.Timestamp = txn.StartTS()
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	} else {
		return SimpleInt(int(value))
	}
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
	object.Timestamp = txn.StartTS()

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
	startTs := txn.StartTS()
	now := oracle.ExtractPhysical(startTs)
	err := getTxnObject(txn, key, object, now, true)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	} else if len(args) == 0 {
		return object.Value
	}
	nowTime := time.Unix(now/1e3, (now%1e3)*1e6)
	opt, err := checkExpireOption(GETEX_COMMAND, args, false, nowTime)
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

	object.Timestamp = txn.StartTS()
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	}
	return object.Value
}
