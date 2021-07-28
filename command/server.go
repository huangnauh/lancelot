package command

import "gitlab.s.upyun.com/platform/lancelot/store"

func (c *Command) FlushAllHandle(txn *store.Txn, args [][]byte) interface{} {
	return OK
}

func (c *Command) FlushDBHandle(txn *store.Txn, args [][]byte) interface{} {
	return OK
}
