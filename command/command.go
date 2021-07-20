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
	ID              int
}

type Command struct {
	cfg       *config.Auth
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

func NewCommand(cfg *config.Config) *Command {
	c := &Command{
		cfg:     &cfg.Auth,
		done:    make(chan struct{}),
		luapool: NewLStatePool(&cfg.Lua),
		scriptMap: &LScriptMap{
			scripts: make(map[string]*lua.FunctionProto),
		},
	}

	c.TxnHandle = map[string]TxnHandler{
		GET_COMMAND: {
			Func:     c.GetHandle,
			ReadOnly: true,
			ID:       0,
		},
		SET_COMMAND: {
			Func: c.SetHandle,
			ID:   1,
		},
		DEL_COMMAND: {
			Func: c.DELHandle,
			ID:   2,
		},
		TTL_COMMAND: {
			Func:     c.TTLHandle,
			ReadOnly: true,
			ID:       3,
		},
		HGET_COMMAND: {
			Func:     c.HGetHandle,
			ReadOnly: true,
			ID:       4,
		},
		HSET_COMMAND: {
			Func: c.HSetHandle,
			ID:   5,
		},
		ACL_COMMAND: {
			Func: c.AclHandle,
			ID:   6,
		},
		EVAL_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_COMMAND)
			},
			NoSupportScript: true,
			ID:              59,
		},
		EVALSHA_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			NoSupportScript: true,
			ID:              60,
		},
		EVAL_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_RO_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
			ID:              61,
		},
		EVALSHA_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
			ID:              62,
		},
		SCRIPT_COMMAND: {
			Func:            c.ScriptHandle,
			NoSupportScript: true,
			ID:              63,
		},
	}
	return c
}
