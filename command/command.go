package command

import (
	"time"

	lua "github.com/yuin/gopher-lua"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/store"
)

type TxnHandle func(txn *store.Txn, args [][]byte) interface{}

type TxnHandler struct {
	Func            TxnHandle
	ReadOnly        bool
	NoSupportScript bool
}

type Command struct {
	done      chan struct{}
	luapool   *LStatePool
	TxnHandle map[string]TxnHandler
	scriptMap *LScriptMap
}

func (c *Command) Shutdown() {
	close(c.done)
	c.luapool.Shutdown()
}

func (c *Command) Start() {
	go c.watchLuaStatePool()
}

func (c *Command) watchLuaStatePool() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			c.luapool.Prune()
		case <-c.done:
			return
		}
	}
}

func NewCommand(cfg *config.Lua) *Command {
	c := &Command{
		done:    make(chan struct{}),
		luapool: NewLStatePool(cfg),
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
			NoSupportScript: true,
		},
		EVALSHA_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			NoSupportScript: true,
		},
		EVAL_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_RO_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
		},
		EVALSHA_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
		},
		SCRIPT_COMMAND: {
			Func:            c.ScriptHandle,
			NoSupportScript: true,
		},
	}
	return c
}
