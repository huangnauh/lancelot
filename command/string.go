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

func getTxnObject(txn *store.Txn, key []byte, object *Object, clear bool) error {
	getType := object.Type
	value, err := txn.Get(key)
	if err != nil {
		return err
	}
	err = ObjectDecode(value, object)
	if err != nil {
		return store.KeyNotFound
	}

	if object.TTL > 0 && time.Unix(object.TTL/1e3, (object.TTL%1e3)*1e6).Before(time.Now()) {
		if clear {
			err = clearTimeout(txn, object)
			if err != nil {
				return err
			}
		}
		object.CleanValue(getType)
		return store.KeyNotFound
	}

	if getType != object.Type {
		return xerror.WrongTypeError
	}
	return nil
}

// https://redis.io/commands/get
// (string) GET key
func (c *Command) GetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(GET_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	} else {
		return object.Value
	}
}

// (string) STRLEN key
func (c *Command) StrLenHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(STRLEN_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	} else {
		return SimpleInt(len(object.Value))
	}
}

func clearTimeout(txn *store.Txn, object *Object) error {
	key := object.GetKeyBytes()
	err := txn.Del(key)
	if err != nil {
		return err
	}
	ttlKey := object.GetTTLKeyBytes()
	err = txn.Del(ttlKey)
	if err != nil {
		return err
	}
	if !object.IsSimple() {
		ttlValue := object.GetTTLValueBytes()
		err = txn.Put(ttlValue, []byte{1})
		if err != nil {
			return err
		}
	}
	return nil
}

// (string) APPEND key value
func (c *Command) AppendHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(APPEND_COMMAND)
	}
	startTs := txn.StartTS()
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
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

type SetOption struct {
	KeepTTL bool
	Get     bool
	Expire  int64
	Check   CheckType
	StartTs uint64
}

func checkSetOption(txn *store.Txn, cmd string, key []byte, args [][]byte) (*Object, *SetOption, error) {
	startTs := txn.StartTS()
	now := oracle.GetTimeFromTS(startTs)
	var expire int64
	var keepTTL, getArg bool
	var check CheckType
	for i := 0; i < len(args); i++ {
		str := strings.ToLower(utils.B2S(args[i]))
		intFlag := false
		var unitDuration time.Duration
		var unitInt64 int64
		switch str {
		case EX:
			if (unitDuration != 0 && unitDuration != time.Second) || keepTTL {
				return nil, nil, xerror.ErrSyntax
			}
			intFlag = true
			unitDuration = time.Second
		case PX:
			if (unitDuration != 0 && unitDuration != time.Millisecond) || keepTTL {
				return nil, nil, xerror.ErrSyntax
			}
			intFlag = true
			unitDuration = time.Millisecond
		case EXAT:
			if (unitInt64 != 0 && unitInt64 != 1000) || keepTTL {
				return nil, nil, xerror.ErrSyntax
			}
			intFlag = true
			unitInt64 = 1000
		case PXAT:
			if (unitInt64 != 0 && unitInt64 != 1) || keepTTL {
				return nil, nil, xerror.ErrSyntax
			}
			intFlag = true
			unitInt64 = 1
		case KEEPTTL:
			if unitDuration > 0 || unitInt64 > 0 {
				return nil, nil, xerror.ErrSyntax
			}
			keepTTL = true
		case NX:
			check = CheckNotExist
		case XX:
			check = CheckExist
		case GET:
			getArg = true
		default:
			return nil, nil, xerror.ErrSyntax
		}

		if intFlag {
			if i+1 == len(args) {
				return nil, nil, xerror.WrongArgsError(cmd)
			}
			intArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return nil, nil, xerror.ErrNotInteger
			}

			if intArg <= 0 {
				return nil, nil, xerror.ErrInvalidExpire
			}

			if unitDuration > 0 {
				expire = now.Add(time.Duration(intArg)*unitDuration).UnixNano() / int64(time.Millisecond)
			} else if unitInt64 > 0 {
				expire = intArg * unitInt64
			}
			i++
		}
	}

	oldObject, err := checkExist(txn, cmd, key, check)
	if err != store.KeyNotFound && err != nil {
		return nil, nil, err
	}

	var oldExpire int64
	if oldObject != nil && oldObject.TTL > 0 {
		oldExpire = oldObject.TTL
	}

	if keepTTL && oldExpire > 0 {
		expire = oldExpire
	}
	keepTTL = expire == oldExpire

	if oldExpire > 0 {
		if !keepTTL {
			ttlKey := oldObject.GetTTLKeyBytes()
			err := txn.Del(ttlKey)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	return oldObject, &SetOption{
		KeepTTL: keepTTL,
		Get:     getArg,
		Expire:  expire,
		Check:   check,
		StartTs: startTs,
	}, nil
}

func checkExist(txn *store.Txn, cmd string, key []byte, check CheckType) (*Object, error) {
	oldObject := NewObject(txn.UserId, txn.DBId, CommandObjectTypes[cmd], key)
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
	_, err := checkExist(txn, SETNX_COMMAND, args[0], CheckNotExist)
	if err == xerror.ErrCheckFailed {
		return SimpleInt(0)
	} else if err != nil && err != store.KeyNotFound {
		return txn.SetError(err)
	}

	startTs := txn.StartTS()
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

// https://redis.io/commands/set
// (string) SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL] [NX|XX] [GET]
func (c *Command) SetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(SET_COMMAND)
	}

	oldObject, setOption, err := checkSetOption(txn, SET_COMMAND, args[0], args[2:])
	if err == xerror.ErrCheckFailed {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}

	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	object.Value = args[1]
	object.TTL = setOption.Expire
	object.Timestamp = setOption.StartTs

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
