package command

import (
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"gitlab.s.upyun.com/platform/lancelot/store"
)

// (generic) TTL key
func TTLHandle(txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) != 1 {
		return txn.LazyWriteWrongArgs(TTL_COMMAND)
	}
	object, err := getTxnObject(txn, KeyType, args[0])
	if err == store.KeyNotFound {
		return txn.LazyWriteInt(-2)
	} else if err != nil {
		return txn.LazyWriteError(err)
	} else if object.TTL > 0 {
		i := (object.TTL - time.Now().UnixNano()/int64(time.Millisecond)) / 1000
		if i > 0 {
			return txn.LazyWriteInt(int(i))
		} else {
			return txn.LazyWriteInt(-2)
		}
	} else {
		return txn.LazyWriteInt(-1)
	}
}

// (generic) DEL key [key ...]
func DELHandle(txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) == 0 {
		return txn.LazyWriteWrongArgs(DEL_COMMAND)
	}
	startTs := txn.StartTS()
	now := oracle.ExtractPhysical(startTs)

	count := 0
	for i := range args {
		object, err := getTxnObject(txn, KeyType, args[i])
		if err == store.KeyNotFound {
			continue
		} else if err != nil {
			return txn.LazyWriteError(err)
		}
		count++
		if object.TTL > 0 {
			ttlKey := object.GetTTLKeyBytes()
			err = txn.Del(ttlKey)
			if err != nil {
				return txn.LazyWriteError(err)
			}
		}

		if !object.IsSimple() {
			object.TTL = now
			ttlKey := object.GetTTLKeyBytes()
			err = txn.Put(ttlKey, []byte{1})
			if err != nil {
				return txn.LazyWriteError(err)
			}
		}

		key := GetKeyBytes(KeyType, object.Key)
		err = txn.Del(key)
		if err != nil {
			return txn.LazyWriteError(err)
		}
	}
	return txn.LazyWriteInt(count)
}
