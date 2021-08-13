package command

import (
	"context"

	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
)

func (c *Command) FlushAllHandle(txn *store.Txn, args [][]byte) interface{} {
	start := GetDataUserPrefix(txn.UserId)
	end := utils.PrefixNext(start)
	ctx := context.Background()
	err := c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}

	start = GetCountUserPrefix(txn.UserId)
	end = utils.PrefixNext(start)
	err = c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}
	return OK
}

func (c *Command) FlushDBHandle(txn *store.Txn, args [][]byte) interface{} {
	start := GetUserDBPrefix(txn.UserId, txn.DBId)
	end := utils.PrefixNext(start)
	ctx := context.Background()
	err := c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}
	return OK
}
