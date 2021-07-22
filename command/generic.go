package command

import (
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"gitlab.s.upyun.com/platform/lancelot/store"
)

// (generic) TTL key
func (c *Command) TTLHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(TTL_COMMAND)
	}
	object, err := getTxnObject(txn, KeyType, args[0])
	if err == store.KeyNotFound {
		return SimpleInt(-2)
	} else if err != nil {
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

func (c *Command) DeleteKeyReturn(txn *store.Txn, object *Object, now int64) interface{} {
	err := c.DeleteKey(txn, object, now)
	if err != nil {
		return txn.SetError(err)
	}
	return 1
}

func (c *Command) DeleteKey(txn *store.Txn, object *Object, now int64) error {
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
		ttlKey := object.GetTTLKeyBytes()
		err = txn.Put(ttlKey, []byte{1})
		if err != nil {
			return err
		}
	}

	key := GetKeyBytes(KeyType, object.Key)
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
		object, err := getTxnObject(txn, KeyType, args[i])
		if err == store.KeyNotFound {
			continue
		} else if err != nil {
			return txn.SetError(err)
		}
		count++
		err = c.DeleteKey(txn, object, now)
		if err != nil {
			return txn.SetError(err)
		}
	}
	return SimpleInt(count)
}
