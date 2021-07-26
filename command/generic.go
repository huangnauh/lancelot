package command

import (
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

// (generic) TTL key
func (c *Command) TTLHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(TTL_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object)
	if err == store.KeyNotFound {
		return SimpleInt(-2)
	} else if err != nil && err != xerror.WrongTypeError {
		return txn.SetError(err)
	} else if object.TTL > 0 {
		i := (object.TTL - time.Now().UnixNano()/int64(time.Millisecond)) / 1000
		if i > 0 {
			return SimpleInt(int(i))
		} else {
			return SimpleInt(-2)
		}
	} else {
		return SimpleInt(-1)
	}
}

func (c *Command) DeleteKeyReturn(txn *store.Txn, key []byte, object *Object, now int64) interface{} {
	err := c.DeleteKey(txn, key, object, now)
	if err != nil {
		return txn.SetError(err)
	}
	return 1
}

func (c *Command) DeleteKey(txn *store.Txn, key []byte, object *Object, now int64) error {
	var err error
	if object.TTL > 0 {
		ttlKey := object.GetTTLKeyBytes()
		err = txn.Del(ttlKey)
		if err != nil {
			return err
		}
	}

	if !object.IsSimple() {
		object.TTL = now
		ttlValue := object.GetTTLValueBytes()
		err = txn.Put(ttlValue, []byte{1})
		if err != nil {
			return err
		}
	}

	err = txn.Del(key)
	if err != nil {
		return err
	}
	return nil
}

// (generic) DEL key [key ...]
func (c *Command) DELHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) == 0 {
		return txn.SetWrongArgs(DEL_COMMAND)
	}
	startTs := txn.StartTS()
	now := oracle.ExtractPhysical(startTs)

	count := 0
	for i := range args {
		object := NewObject(txn.UserId, txn.DBId, KeyType, args[i])
		key := object.GetKeyBytes()
		err := getTxnObject(txn, key, object)
		if err == store.KeyNotFound {
			continue
		} else if err != nil && err != xerror.WrongTypeError {
			return txn.SetError(err)
		}
		count++
		err = c.DeleteKey(txn, key, object, now)
		if err != nil {
			return txn.SetError(err)
		}
	}
	return SimpleInt(count)
}
