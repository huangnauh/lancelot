package server

import (
	"time"

	"gitlab.s.upyun.com/platform/lancelot/store"
)

// (generic) TTL key
func TTL(txn *store.Txn, args [][]byte) store.RespFunc {
	if len(args) != 1 {
		return txn.LazyWriteWrongArgs(TTL_COMMAND)
	}
	key := GetKeyBytes(StringPrefix, args[0])
	_, expire, err := getTxnKey(txn, key)
	if err == store.KeyNotFound {
		return txn.LazyWriteInt(-2)
	} else if err != nil {
		return txn.LazyWriteError(err)
	} else if expire > 0 {
		i := (expire - time.Now().UnixNano()/int64(time.Millisecond)) / 1000
		if i > 0 {
			return txn.LazyWriteInt(int(i))
		} else {
			return txn.LazyWriteInt(-2)
		}
	} else {
		return txn.LazyWriteInt(-1)
	}
}
