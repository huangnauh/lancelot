package command

import (
	"github.com/google/uuid"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

//(hash) HEXISTS key field
func (c *Command) HExistsHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(HEXISTS_COMMAND)
	}
	field := args[1]
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return 0
	} else if err != nil {
		return txn.SetError(err)
	}
	hkey := object.GetKeyFieldBytes(field)
	_, err = txn.Get(hkey)
	if err == store.KeyNotFound {
		return 0
	} else if err != nil {
		return txn.SetError(err)
	}
	return 1
}

//(hash) HGET key field
func (c *Command) HGetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(HGET_COMMAND)
	}
	field := args[1]
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
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

//(hash) HDEL key field [field ...]
func (c *Command) HDelHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(HDEL_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	} else if object.Type != HashType {
		return txn.SetError(xerror.WrongTypeError)
	}

	ret := 0
	for start := 1; start < len(args); start++ {
		hkey := object.GetKeyFieldBytes(args[start])
		_, err = txn.Get(hkey)
		if err == store.KeyNotFound {
			continue
		} else if err != nil {
			return txn.SetError(err)
		}

		ret++
		err = txn.Del(hkey)
		if err != nil {
			return txn.SetError(err)
		}
	}
	return SimpleInt(ret)
}

//(hash) HSET key field value [field value ...]
func (c *Command) HSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 || len(args)%2 != 1 {
		return txn.SetWrongArgs(HSET_COMMAND)
	}
	ret := 0
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		id, err := uuid.NewUUID()
		if err != nil {
			return txn.SetError(err)
		}
		object.Value = id[:]
	} else if err != nil {
		return txn.SetError(err)
	} else if object.Type != HashType {
		return txn.SetError(xerror.WrongTypeError)
	}

	object.Count++
	object.Timestamp = txn.Timestamp

	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	}

	for start := 1; start < len(args); start += 2 {
		hkey := object.GetKeyFieldBytes(args[start])
		_, err = txn.Get(hkey)
		if err == store.KeyNotFound {
			ret++
		} else if err != nil {
			return txn.SetError(err)
		}

		err = txn.Put(hkey, args[start+1])
		if err != nil {
			return txn.SetError(err)
		}
	}

	return SimpleInt(ret)
}
