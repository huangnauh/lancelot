package command

import "github.com/huangnauh/lancelot/store"

// COPY source destination [DB destination-db] [REPLACE]
func (c *Command) CopyHandle(txn *store.Txn, args [][]byte) interface{} {
	return nil
}
