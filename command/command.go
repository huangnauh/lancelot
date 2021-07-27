package command

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	lua "github.com/yuin/gopher-lua"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
)

type TxnHandle func(txn *store.Txn, args [][]byte) interface{}
type ConnHandle func(conn *redcon.Conn, cmd redcon.Command)

type TxnHandler struct {
	Func            TxnHandle
	ReadOnly        bool
	NoSupportScript bool
	ID              int
}

type ConnHandler struct {
	Func ConnHandle
	ID   int
}

type Command struct {
	cfg        *config.Config
	done       chan struct{}
	luapool    *LStatePool
	TxnHandle  map[string]TxnHandler
	ConnHandle map[string]ConnHandler
	scriptMap  *LScriptMap
	users      map[string]*User
	root       *User
	ulock      sync.RWMutex
	client     *store.Client
	gcWait     *sync.WaitGroup
	gcWorkers  int32
	gcClosed   chan bool
	cache      *cache.Cache
}

func NewCommand(cfg *config.Config) *Command {
	c := &Command{
		cfg:     cfg,
		done:    make(chan struct{}),
		luapool: NewLStatePool(&cfg.Lua),
		scriptMap: &LScriptMap{
			scripts: make(map[string]*lua.FunctionProto),
		},
		users:    make(map[string]*User),
		gcClosed: make(chan bool),
		gcWait:   &sync.WaitGroup{},
		cache:    cache.New(cfg.Key.CursorExpiration, cfg.Key.CursorExpiration/2),
	}
	c.root = c.RootUser()

	c.ConnHandle = map[string]ConnHandler{
		WATCH_COMMAND: {
			Func: c.watch,
			ID:   512,
		},
		MULTI_COMMAND: {
			Func: c.multi,
			ID:   511,
		},
		EXEC_COMMAND: {
			Func: c.exec,
			ID:   510,
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
		HDEL_COMMAND: {
			Func: c.HDelHandle,
			ID:   6,
		},
		HEXISTS_COMMAND: {
			Func:     c.HExistsHandle,
			ID:       7,
			ReadOnly: true,
		},
		SCAN_COMMAND: {
			Func:     c.ScanHandle,
			ReadOnly: true,
			ID:       62,
		},
		ACL_COMMAND: {
			Func: c.AclHandle,
			ID:   63,
		},
		AUTH_COMMAND: {
			Func:            c.AuthHandle,
			ID:              127,
			NoSupportScript: true,
		},
		EVAL_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_COMMAND)
			},
			NoSupportScript: true,
			ID:              254,
		},
		EVALSHA_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			NoSupportScript: true,
			ID:              255,
		},
		EVAL_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_RO_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
			ID:              509,
		},
		EVALSHA_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
			ID:              510,
		},
		SCRIPT_COMMAND: {
			Func:            c.ScriptHandle,
			NoSupportScript: true,
			ID:              511,
		},
		JSONSET_COMMAND: {
			Func: c.JsonSetHandle,
			ID:   1023,
		},
		JSONGET_COMMAND: {
			Func: c.JsonGetHandle,
			ID:   1022,
		},
		JSONDEL_COMMAND: {
			Func: c.JsonDelHandle,
			ID:   1021,
		},
	}
	return c
}

func (c *Command) Shutdown(ctx context.Context) {
	close(c.done)
	c.luapool.Shutdown()
	select {
	case <-c.gcClosed:
	case <-ctx.Done():
	}
}

func (c *Command) Start() error {
	var err error
	c.client, err = store.Open(&c.cfg.Store)
	if err != nil {
		return err
	}

	go c.watchLuaStatePool()
	go c.watchUser()
	go c.startGC()
	return nil
}

func (c *Command) SetLocalUsers(users map[string]*User) {
	c.ulock.Lock()
	c.users = users
	c.users[c.root.Name] = c.root
	c.ulock.Unlock()
}

func (c *Command) SetLocalUser(user *User) {
	c.ulock.Lock()
	c.users[user.Name] = user
	c.ulock.Unlock()
}

func (c *Command) GetLocalUser(username string) (*User, bool) {
	c.ulock.RLock()
	user, ok := c.users[username]
	c.ulock.RUnlock()
	return user, ok
}

func (c *Command) GetLocalUsers() []*User {
	c.ulock.RLock()
	users := make([]*User, 0, len(c.users))
	for _, user := range c.users {
		users = append(users, user)
	}
	c.ulock.RUnlock()
	return users
}

func (c *Command) watchUser() {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		users, err := c.ListUsers()
		if err != nil {
			logrus.Errorf("watchUser: %s", err)
			continue
		}
		c.SetLocalUsers(users)
		select {
		case <-t.C:
		case <-c.done:
			return
		}
	}
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

func (c *Command) GetCursor(cursor uint64) ([]byte, bool) {
	key := fmt.Sprintf("c%d", cursor)
	v, ok := c.cache.Get(key)
	if !ok {
		return nil, false
	}
	return v.([]byte), true
}

func (c *Command) SetCursor(cursor uint64, data []byte) {
	key := fmt.Sprintf("c%d", cursor)
	c.cache.Set(key, data, c.cfg.Key.CursorExpiration)
}
