package command

import (
	"github.com/google/uuid"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

//(hash) HGET key field
func (c *Command) HGetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(HGET_COMMAND)
	}
	field := args[1]
	object, err := getTxnObject(txn, KeyType, args[0])
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	} else if object.Type != HashType {
		return txn.SetError(xerror.WrongTypeError)
	}
	hkey := object.GetKeyBytes(field)
	value, err := txn.Get(hkey)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	return value
}

//(hash) HSET key field value
func (c *Command) HSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(HSET_COMMAND)
	}
	startTs := txn.StartTS()
	field := args[1]
	value := args[2]
	ret := 0
	object, err := getTxnObject(txn, KeyType, args[0])
	if err == store.KeyNotFound {
		id, err := uuid.NewUUID()
		if err != nil {
			return txn.SetError(err)
		}
		object = &Object{
			Key:       args[0],
			Type:      HashType,
			Timestamp: startTs,
			Value:     id[:],
		}
		ret = 1
		key := GetKeyBytes(KeyType, object.Key)
		err = txn.Put(key, ObjectEncode(object))
		if err != nil {
			return txn.SetError(err)
		}
	} else if err != nil {
		return txn.SetError(err)
	} else if object.Type != HashType {
		return txn.SetError(xerror.WrongTypeError)
	}

	hkey := object.GetKeyBytes(field)
	_, err = txn.Get(hkey)
	if err == store.KeyNotFound {
		ret = 1
	} else if err != nil {
		return txn.SetError(err)
	}

	err = txn.Put(hkey, value)
	if err != nil {
		return txn.SetError(err)
	}
	return SimpleInt(ret)
}
