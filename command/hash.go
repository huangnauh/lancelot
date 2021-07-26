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
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	hkey := object.GetKeyFieldBytes(field)
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
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object)
	if err == store.KeyNotFound {
		id, err := uuid.NewUUID()
		if err != nil {
			return txn.SetError(err)
		}
		object.Timestamp = startTs
		object.Value = id[:]
		ret = 1
		err = txn.Put(key, ObjectEncode(object))
		if err != nil {
			return txn.SetError(err)
		}
	} else if err != nil {
		return txn.SetError(err)
	} else if object.Type != HashType {
		return txn.SetError(xerror.WrongTypeError)
	}

	hkey := object.GetKeyFieldBytes(field)
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
