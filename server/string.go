package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"gitlab.s.upyun.com/platform/lancelot/store"
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

func getTxnKey(txn *store.Txn, key []byte) ([]byte, int64, error) {
	value, err := txn.Get(key)
	if err != nil {
		return value, 0, err
	}
	object := &Object{}
	err = ObjectDecode(value, object)
	if err != nil {
		return nil, object.TTL, store.KeyNotFound
	}

	if object.Type != KeyType {
		return nil, object.TTL, wrongTypeError
	}

	if object.TTL > 0 && time.Unix(object.TTL/1000, object.TTL%1000).Before(time.Now()) {
		return nil, 0, store.KeyNotFound
	}

	return object.Value, object.TTL, nil
}

// (string) GET key
func GetHandle(txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) != 1 {
		return txn.LazyWriteWrongArgs(GET_COMMAND)
	}
	key := GetKeyBytes(KeyType, args[0])
	v, _, err := getTxnKey(txn, key)
	if err == store.KeyNotFound {
		return txn.LazyWriteNull()
	} else if err != nil {
		return txn.LazyWriteError(err)
	} else {
		return txn.LazyWriteString(string(v))
	}
}

// https://redis.io/commands/set
// (string) SET key value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL] [NX|XX] [GET]
func SetHandle(txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) < 2 {
		txn.Err = wrongNumberOfArgs
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
				return txn.LazyWriteError(errSyntax)
			}
			intFlag = true
			unitDuration = time.Second
		case PX:
			if (unitDuration != 0 && unitDuration != time.Millisecond) || keepTTL {
				return txn.LazyWriteError(errSyntax)
			}
			intFlag = true
			unitDuration = time.Millisecond
		case EXAT:
			if (unitInt64 != 0 && unitInt64 != 1000) || keepTTL {
				return txn.LazyWriteError(errSyntax)
			}
			intFlag = true
			unitInt64 = 1000
		case PXAT:
			if (unitInt64 != 0 && unitInt64 != 1) || keepTTL {
				return txn.LazyWriteError(errSyntax)
			}
			intFlag = true
			unitInt64 = 1
		case KEEPTTL:
			if unitDuration > 0 || unitInt64 > 0 {
				return txn.LazyWriteError(errSyntax)
			}
			keepTTL = true
		case NX:
			check = CheckNotExist
		case XX:
			check = CheckExist
		case GET:
			getArg = true
		default:
			return txn.LazyWriteError(errSyntax)
		}

		if intFlag {
			if i+1 == len(args) {
				return txn.LazyWriteWrongArgs(SET_COMMAND)
			}
			intArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return txn.LazyWriteError(errNotInteger)
			}

			if intArg <= 0 {
				return txn.LazyWriteError(errInvalidExpire)
			}

			if unitDuration > 0 {
				expire = now.Add(time.Duration(intArg)*unitDuration).UnixNano() / int64(time.Millisecond)
			} else if unitInt64 > 0 {
				expire = intArg * unitInt64
			}
			i++
		}
	}

	var oldValue []byte
	var oldExpire int64
	var getOld bool
	var getErr error
	key := GetKeyBytes(KeyType, args[0])

	if expire > 0 {
		ttlKey := GetTTLBytes(expire, KeyType, key)
		err := txn.Put(ttlKey, []byte{1})
		if err != nil {
			return txn.LazyWriteError(err)
		}
	} else {
		getOld = true
		oldValue, oldExpire, getErr = getTxnKey(txn, key)
		if getErr != nil && getErr != store.KeyNotFound {
			return txn.LazyWriteError(getErr)
		}

		if keepTTL {
			expire = oldExpire
		} else if oldExpire > 0 {
			ttlKey := GetTTLBytes(oldExpire, KeyType, key)
			err := txn.Del(ttlKey)
			if err != nil {
				return txn.LazyWriteError(err)
			}
		}
	}

	if check > 0 || getArg {
		if !getOld {
			oldValue, _, getErr = getTxnKey(txn, key)
		}
		if getErr == store.KeyNotFound {
			if CheckExist == check {
				return txn.LazyWriteNull()
			}
		} else if getErr != nil {
			return txn.LazyWriteError(getErr)
		} else {
			if CheckNotExist == check {
				return txn.LazyWriteNull()
			}
		}
	}

	value := args[1]
	object := &Object{
		Type:      KeyType,
		TTL:       expire,
		Timestamp: startTs,
		Value:     value,
	}
	err := txn.Put(key, ObjectEncode(object))
	if err != nil {
		txn.Err = err
		return txn.LazyWriteError(err)
	} else if getOld {
		if oldValue != nil {
			return txn.LazyWriteString(string(oldValue))
		} else {
			return txn.LazyWriteNull()
		}
	} else {
		return txn.LazyWriteString(OK)
	}
}
