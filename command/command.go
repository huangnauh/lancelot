package command

import (
	lua "github.com/yuin/gopher-lua"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/store"
)

type TxnHandle func(txn *store.Txn, args [][]byte) interface{}

type TxnHandler struct {
	Func     TxnHandle
	ReadOnly bool
}

type Command struct {
	luapool   *LStatePool
	TxnHandle map[string]TxnHandler
	scriptMap *LScriptMap
}

func NewCommand(cfg *config.Lua) *Command {
	c := &Command{
		luapool: NewLStatePool(cfg.InitPoolSize, cfg.MaxPoolSize),
		scriptMap: &LScriptMap{
			scripts: make(map[string]*lua.FunctionProto),
		},
	}

	c.TxnHandle = map[string]TxnHandler{
		GET_COMMAND: {
			Func:     c.GetHandle,
			ReadOnly: true,
		},
		SET_COMMAND: {
			Func: c.SetHandle,
		},
		DEL_COMMAND: {
			Func: c.DELHandle,
		},
		TTL_COMMAND: {
			Func:     c.TTLHandle,
			ReadOnly: true,
		},
		HGET_COMMAND: {
			Func:     c.HGetHandle,
			ReadOnly: true,
		},
		HSET_COMMAND: {
			Func: c.HSetHandle,
		},
		EVAL_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_COMMAND)
			},
		},
		EVALSHA_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
		},
		EVAL_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_RO_COMMAND)
			},
			ReadOnly: true,
		},
		EVALSHA_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			ReadOnly: true,
		},
	}
	return c
}
