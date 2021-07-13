package command

import (
	"github.com/google/uuid"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

//(hash) HGET key field
func HGetHandle(txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) != 2 {
		return txn.LazyWriteWrongArgs(HGET_COMMAND)
	}
	key := GetKeyBytes(KeyType, args[0])
	field := args[1]
	object, err := getTxnObject(txn, key)
	if err == store.KeyNotFound {
		return txn.LazyWriteNull()
	} else if err != nil {
		return txn.LazyWriteError(err)
	} else if object.Type != HashType {
		return txn.LazyWriteError(xerror.WrongTypeError)
	}
	hkey := object.GetHashBytes(field)
	value, err := txn.Get(hkey)
	if err == store.KeyNotFound {
		return txn.LazyWriteNull()
	} else if err != nil {
		return txn.LazyWriteError(err)
	}
	return txn.LazyWriteBulk(value)
}

//(hash) HSET key field value
func HSetHandle(txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) != 3 {
		return txn.LazyWriteWrongArgs(HSET_COMMAND)
	}
	startTs := txn.StartTS()
	key := GetKeyBytes(KeyType, args[0])
	field := args[1]
	value := args[2]
	ret := 0
	object, err := getTxnObject(txn, key)
	if err == store.KeyNotFound {
		id, err := uuid.NewUUID()
		if err != nil {
			return txn.LazyWriteError(err)
		}
		object = &Object{
			Type:      HashType,
			Timestamp: startTs,
			Value:     id[:],
		}
		ret = 1
		err = txn.Put(key, ObjectEncode(object))
		if err != nil {
			return txn.LazyWriteError(err)
		}
	} else if err != nil {
		return txn.LazyWriteError(err)
	} else if object.Type != HashType {
		return txn.LazyWriteError(xerror.WrongTypeError)
	}

	hkey := object.GetHashBytes(field)
	_, err = txn.Get(hkey)
	if err == store.KeyNotFound {
		ret = 1
	} else if err != nil {
		return txn.LazyWriteError(err)
	}

	err = txn.Put(hkey, value)
	if err != nil {
		return txn.LazyWriteError(err)
	}
	return txn.LazyWriteInt(ret)
}
