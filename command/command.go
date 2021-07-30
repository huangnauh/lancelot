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
	Type            ObjectType
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
	Root       *User
	Default    *User
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
	c.Root = c.rootUser()
	c.users[c.Root.Name] = c.Root
	c.Default = c.defaultUser()
	c.users[c.Default.Name] = c.Default

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
			Type:     KeyType,
		},
		SET_COMMAND: {
			Func: c.SetHandle,
			ID:   1,
			Type: KeyType,
		},
		GETSET_COMMAND: {
			Func: c.GetSetHandle,
			ID:   2,
			Type: KeyType,
		},
		SETNX_COMMAND: {
			Func: c.SetNXHandle,
			ID:   3,
			Type: KeyType,
		},
		STRLEN_COMMAND: {
			Func:     c.StrLenHandle,
			ID:       4,
			ReadOnly: true,
			Type:     KeyType,
		},
		APPEND_COMMAND: {
			Func: c.AppendHandle,
			ID:   5,
			Type: KeyType,
		},
		DECR_COMMAND: {
			Func: c.DecrHandle,
			ID:   6,
			Type: KeyType,
		},
		DECRBY_COMMAND: {
			Func: c.DecrByHandle,
			ID:   7,
			Type: KeyType,
		},
		INCR_COMMAND: {
			Func: c.IncrHandle,
			ID:   8,
			Type: KeyType,
		},
		INCRBY_COMMAND: {
			Func: c.IncrByHandle,
			ID:   9,
			Type: KeyType,
		},
		GETDEL_COMMAND: {
			Func: c.GetDelHandle,
			ID:   10,
			Type: KeyType,
		},
		GETEX_COMMAND: {
			Func: c.GetExHandle,
			ID:   11,
			Type: KeyType,
		},
		GETRANGE_COMMAND: {
			Func:     c.GetRangeHandle,
			ID:       12,
			ReadOnly: true,
			Type:     KeyType,
		},
		INCRBYFLOAT_COMMAND: {
			Func: c.IncrByFloatHandle,
			ID:   13,
			Type: KeyType,
		},
		SETEX_COMMAND: {
			Func: c.SetExHandle,
			ID:   14,
			Type: KeyType,
		},
		MGET_COMMAND: {
			Func:     c.MGetHandle,
			ID:       15,
			Type:     KeyType,
			ReadOnly: true,
		},
		MSET_COMMAND: {
			Func: c.MSetHandle,
			ID:   16,
			Type: KeyType,
		},
		MSETNX_COMMAND: {
			Func: c.MSetNXHandle,
			ID:   17,
			Type: KeyType,
		},
		PSETEX_COMMAND: {
			Func: c.PSetExHandle,
			ID:   18,
			Type: KeyType,
		},
		DEL_COMMAND: {
			Func: c.DELHandle,
			ID:   31,
			Type: KeyType,
		},
		TTL_COMMAND: {
			Func:     c.TTLHandle,
			ReadOnly: true,
			ID:       32,
			Type:     KeyType,
		},
		EXPIRE_COMMAND: {
			Func: c.ExpireHandle,
			ID:   33,
			Type: KeyType,
		},
		EXISTS_COMMAND: {
			Func:     c.ExistsHandle,
			ReadOnly: true,
			ID:       34,
			Type:     KeyType,
		},
		HGET_COMMAND: {
			Func:     c.HGetHandle,
			ReadOnly: true,
			ID:       48,
			Type:     HashType,
		},
		HSET_COMMAND: {
			Func: c.HSetHandle,
			ID:   49,
			Type: HashType,
		},
		HDEL_COMMAND: {
			Func: c.HDelHandle,
			ID:   50,
			Type: HashType,
		},
		HEXISTS_COMMAND: {
			Func:     c.HExistsHandle,
			ID:       51,
			ReadOnly: true,
			Type:     HashType,
		},
		FLUSHALL_COMMAND: {
			Func: c.FlushAllHandle,
			ID:   64,
			Type: UnknownType,
		},
		FLUSHDB_COMMAND: {
			Func: c.FlushDBHandle,
			ID:   65,
			Type: UnknownType,
		},
		SCAN_COMMAND: {
			Func:     c.ScanHandle,
			ReadOnly: true,
			ID:       66,
			Type:     KeyType,
		},
		ACL_COMMAND: {
			Func: c.AclHandle,
			ID:   67,
			Type: UserType,
		},
		AUTH_COMMAND: {
			Func:            c.AuthHandle,
			ID:              127,
			NoSupportScript: true,
			Type:            UserType,
		},
		EVAL_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_COMMAND)
			},
			NoSupportScript: true,
			ID:              254,
			Type:            UnknownType,
		},
		EVALSHA_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			NoSupportScript: true,
			ID:              255,
			Type:            UnknownType,
		},
		EVAL_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_RO_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
			ID:              509,
			Type:            UnknownType,
		},
		EVALSHA_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
			ID:              510,
			Type:            UnknownType,
		},
		SCRIPT_COMMAND: {
			Func:            c.ScriptHandle,
			NoSupportScript: true,
			ID:              511,
			Type:            UnknownType,
		},
		JSONSET_COMMAND: {
			Func: c.JsonSetHandle,
			ID:   1023,
			Type: UnknownType,
		},
		JSONGET_COMMAND: {
			Func: c.JsonGetHandle,
			ID:   1022,
			Type: UnknownType,
		},
		JSONDEL_COMMAND: {
			Func: c.JsonDelHandle,
			ID:   1021,
			Type: UnknownType,
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

func (c *Command) GetClient() *store.Client {
	return c.client
}

func (c *Command) SetLocalUsers(users map[string]*User) {
	c.ulock.Lock()
	c.users = users
	c.users[c.Root.Name] = c.Root
	c.users[c.Default.Name] = c.Default
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
