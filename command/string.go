package command

import (
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"gitlab.s.upyun.com/platform/lancelot/store"
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
	KEEPTTL = "keepall"
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

func getTxnObject(txn *store.Txn, objectType ObjectType, origin []byte) (*Object, error) {
	key := GetKeyBytes(KeyType, origin)
	value, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	object := &Object{Key: origin, Type: objectType}
	err = ObjectDecode(value, object)
	if err != nil {
		return nil, store.KeyNotFound
	}

	if object.TTL > 0 && time.Unix(object.TTL/1e3, (object.TTL%1e3)*1e6).Before(time.Now()) {
		return nil, store.KeyNotFound
	}
	return object, nil
}

func getTxnKey(txn *store.Txn, objectType ObjectType, origin []byte) (*Object, error) {
	object, err := getTxnObject(txn, objectType, origin)
	if err != nil {
		return nil, err
	}

	if object.Type != objectType {
		return nil, xerror.WrongTypeError
	}
	return object, nil
}

// https://redis.io/commands/get
// (string) GET key
func GetHandle(txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) != 1 {
		return txn.LazyWriteWrongArgs(GET_COMMAND)
	}
	object, err := getTxnKey(txn, KeyType, args[0])
	if err == store.KeyNotFound {
		return txn.LazyWriteNull()
	} else if err != nil {
		return txn.LazyWriteError(err)
	} else {
		return txn.LazyWriteBulk(object.Value)
	}
}

// https://redis.io/commands/set
// (string) SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL] [NX|XX] [GET]
func SetHandle(txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) < 2 {
		return txn.LazyWriteWrongArgs(SET_COMMAND)
	}

	startTs := txn.StartTS()
	now := oracle.GetTimeFromTS(startTs)
	var expire int64
	var keepTTL, getArg bool
	var check CheckType
	for i := 2; i < len(args); i++ {
		str := strings.ToLower(string(args[i]))
		intFlag := false
		var unitDuration time.Duration
		var unitInt64 int64
		switch str {
		case EX:
			if (unitDuration != 0 && unitDuration != time.Second) || keepTTL {
				return txn.LazyWriteError(xerror.ErrSyntax)
			}
			intFlag = true
			unitDuration = time.Second
		case PX:
			if (unitDuration != 0 && unitDuration != time.Millisecond) || keepTTL {
				return txn.LazyWriteError(xerror.ErrSyntax)
			}
			intFlag = true
			unitDuration = time.Millisecond
		case EXAT:
			if (unitInt64 != 0 && unitInt64 != 1000) || keepTTL {
				return txn.LazyWriteError(xerror.ErrSyntax)
			}
			intFlag = true
			unitInt64 = 1000
		case PXAT:
			if (unitInt64 != 0 && unitInt64 != 1) || keepTTL {
				return txn.LazyWriteError(xerror.ErrSyntax)
			}
			intFlag = true
			unitInt64 = 1
		case KEEPTTL:
			if unitDuration > 0 || unitInt64 > 0 {
				return txn.LazyWriteError(xerror.ErrSyntax)
			}
			keepTTL = true
		case NX:
			check = CheckNotExist
		case XX:
			check = CheckExist
		case GET:
			getArg = true
		default:
			return txn.LazyWriteError(xerror.ErrSyntax)
		}

		if intFlag {
			if i+1 == len(args) {
				return txn.LazyWriteWrongArgs(SET_COMMAND)
			}
			intArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return txn.LazyWriteError(xerror.ErrNotInteger)
			}

			if intArg <= 0 {
				return txn.LazyWriteError(xerror.ErrInvalidExpire)
			}

			if unitDuration > 0 {
				expire = now.Add(time.Duration(intArg)*unitDuration).UnixNano() / int64(time.Millisecond)
			} else if unitInt64 > 0 {
				expire = intArg * unitInt64
			}
			i++
		}
	}

	oldObject, err := getTxnKey(txn, KeyType, args[0])
	if err == store.KeyNotFound {
		if CheckExist == check {
			return txn.LazyWriteNull()
		}
	} else if err != nil && err != store.KeyNotFound {
		return txn.LazyWriteError(err)
	} else {
		if CheckNotExist == check {
			return txn.LazyWriteNull()
		}
	}

	var oldExpire int64
	if oldObject != nil && oldObject.TTL > 0 {
		oldExpire = oldObject.TTL
	}

	if keepTTL && oldExpire > 0 {
		expire = oldExpire
	}

	object := &Object{
		Key:       args[0],
		Type:      KeyType,
		TTL:       expire,
		Timestamp: startTs,
		Value:     args[1],
	}

	if expire > 0 && expire != oldExpire {
		ttlKey := object.GetTTLKeyBytes()
		err := txn.Put(ttlKey, []byte{1})
		if err != nil {
			return txn.LazyWriteError(err)
		}
	}

	if oldExpire > 0 && expire != oldExpire {
		ttlKey := oldObject.GetTTLKeyBytes()
		err := txn.Del(ttlKey)
		if err != nil {
			return txn.LazyWriteError(err)
		}
	}

	key := GetKeyBytes(KeyType, object.Key)
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.LazyWriteError(err)
	} else if getArg {
		if oldObject != nil && oldObject.Value != nil {
			return txn.LazyWriteString(string(oldObject.Value))
		} else {
			return txn.LazyWriteNull()
		}
	} else {
		return txn.LazyWriteString(OK)
	}
}
