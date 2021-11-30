package command

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coocood/freecache"
	lua "github.com/yuin/gopher-lua"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/member"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
)

type TxnHandle func(txn *store.Txn, args [][]byte) interface{}
type ConnHandle func(conn *redcon.Conn, cmd redcon.Command)

type TxnHandler struct {
	Name            string
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

type Info struct {
	ConnectedClients int64
	BlockClients     int64
}

type Command struct {
	cfg        *config.Config
	red        *redcon.Server
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
	psManager  *PsManager
	memberlist *member.MemberList
	cache      *freecache.Cache
	Info       *Info
}

func (c *Command) Shutdown(ctx context.Context) {
	close(c.done)
	if c.memberlist != nil {
		c.memberlist.Close()
	}
	c.luapool.Shutdown()
	c.client.Close()
	for {
		select {
		case _, ok := <-c.gcClosed:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *Command) Start() error {
	var err error
	c.client, err = store.Open(c.cfg)
	if err != nil {
		return err
	}
	etcdCtl := c.client.GetEtcdCtl()
	if etcdCtl != nil {
		c.memberlist = member.NewMemberList(etcdCtl, fmt.Sprintf("%s:%d", c.cfg.Host, c.cfg.RpcPort))
		err = c.memberlist.Start()
		if err != nil {
			return err
		}
	}
	c.psManager = NewPsManager(&c.cfg.PubSub, c.memberlist)

	go c.watchLuaStatePool()
	go c.watchUser()
	go c.startGC()
	return nil
}

func (c *Command) GetClient() *store.Client {
	return c.client
}

func (c *Command) GetCurrentID() (uint64, error) {
	return c.client.CurrentVersion()
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

func (c *Command) DelLocalUser(username string) {
	c.ulock.Lock()
	delete(c.users, username)
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
			utils.ZapLog.Error("watchUser", zap.Error(err))
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

func (c *Command) GetCachedScript() int {
	return c.scriptMap.Len()
}

func (c *Command) GetCursor(key string) ([]byte, bool) {
	utils.ZapLog.Debug("GetCursor", zap.String("key", key))
	v, err := c.cache.Get(utils.S2B(key))
	if err != nil {
		return nil, false
	}
	return v, true
}

func (c *Command) SetCursor(key string, data []byte) {
	utils.ZapLog.Debug("SetCursor", zap.String("key", key), zap.ByteString("value", data))
	_ = c.cache.Set(utils.S2B(key), data, c.cfg.Key.CursorExpireSecond)
}

func (c *Command) Accept(conn *redcon.Conn) bool {
	atomic.AddInt64(&c.Info.ConnectedClients, 1)
	utils.ZapLog.Debug("Accept", zap.String("remote", conn.RemoteAddr()))
	// id, err := c.GetCurrentID()
	// if err != nil {
	// 	id = oracle.GoTimeToTS(time.Now())
	// }
	// conn.ID = id
	return true
}

func (c *Command) Close(conn *redcon.Conn, err error) {
	utils.ZapLog.Info("Close", zap.String("remote", conn.RemoteAddr()), zap.Error(err))
	atomic.AddInt64(&c.Info.ConnectedClients, -1)
	connTxn := conn.Transaction()
	if connTxn != nil {
		txn, ok := connTxn.(*store.Txn)
		if ok {
			txn.Rollback()
		}
	}
}

func NewCommand(cfg *config.Config, red *redcon.Server) *Command {
	c := &Command{
		cfg:     cfg,
		red:     red,
		done:    make(chan struct{}),
		luapool: NewLStatePool(&cfg.Lua),
		scriptMap: &LScriptMap{
			scripts: make(map[string]*lua.FunctionProto),
		},
		users:    make(map[string]*User),
		gcClosed: make(chan bool, 1),
		gcWait:   &sync.WaitGroup{},
		cache:    freecache.NewCache(cfg.CacheSize),
		Info:     &Info{},
	}
	c.Root = c.rootUser()
	c.users[c.Root.Name] = c.Root
	c.Default = c.defaultUser()
	c.users[c.Default.Name] = c.Default

	c.ConnHandle = map[string]ConnHandler{
		WATCH_COMMAND: {
			Func: c.watch,
			ID:   1023,
		},
		MULTI_COMMAND: {
			Func: c.multi,
			ID:   1022,
		},
		EXEC_COMMAND: {
			Func: c.exec,
			ID:   1021,
		},
		DISCARD_COMMAND: {
			Func: c.discard,
			ID:   1020,
		},
		SUBSCRIBE_COMMAND: {
			Func: c.SubscribeHandle,
			ID:   352,
		},
		PSUBSCRIBE_COMMAND: {
			Func: c.PSubscribeHandle,
			ID:   353,
		},
		UNSUBSCRIBE_COMMAND: {
			Func: c.UnsubscribeHandle,
			ID:   354,
		},
		PUNSUBSCRIBE_COMMAND: {
			Func: c.PUnsubscribeHandle,
			ID:   355,
		},
	}

	c.TxnHandle = map[string]TxnHandler{
		// ------------------- string end 0 ------------------------------------
		GET_COMMAND: {
			Name:     GET_COMMAND,
			Func:     c.GetHandle,
			ReadOnly: true,
			ID:       0,
			Type:     StringType,
		},
		SET_COMMAND: {
			Name: SET_COMMAND,
			Func: c.SetHandle,
			ID:   1,
			Type: StringType,
		},
		GETSET_COMMAND: {
			Name: GETSET_COMMAND,
			Func: c.GetSetHandle,
			ID:   2,
			Type: StringType,
		},
		SETNX_COMMAND: {
			Name: SETNX_COMMAND,
			Func: c.SetNXHandle,
			ID:   3,
			Type: StringType,
		},
		STRLEN_COMMAND: {
			Name:     STRLEN_COMMAND,
			Func:     c.StrLenHandle,
			ID:       4,
			ReadOnly: true,
			Type:     StringType,
		},
		APPEND_COMMAND: {
			Name: APPEND_COMMAND,
			Func: c.AppendHandle,
			ID:   5,
			Type: StringType,
		},
		DECR_COMMAND: {
			Name: DECR_COMMAND,
			Func: c.DecrHandle,
			ID:   6,
			Type: StringType,
		},
		DECRBY_COMMAND: {
			Name: DECRBY_COMMAND,
			Func: c.DecrByHandle,
			ID:   7,
			Type: StringType,
		},
		INCR_COMMAND: {
			Name: INCR_COMMAND,
			Func: c.IncrHandle,
			ID:   8,
			Type: StringType,
		},
		INCRBY_COMMAND: {
			Name: INCRBY_COMMAND,
			Func: c.IncrByHandle,
			ID:   9,
			Type: StringType,
		},
		GETDEL_COMMAND: {
			Name: GETDEL_COMMAND,
			Func: c.GetDelHandle,
			ID:   10,
			Type: StringType,
		},
		GETEX_COMMAND: {
			Name: GETEX_COMMAND,
			Func: c.GetExHandle,
			ID:   11,
			Type: StringType,
		},
		GETRANGE_COMMAND: {
			Name:     GETRANGE_COMMAND,
			Func:     c.GetRangeHandle,
			ID:       12,
			ReadOnly: true,
			Type:     StringType,
		},
		INCRBYFLOAT_COMMAND: {
			Name: INCRBYFLOAT_COMMAND,
			Func: c.IncrByFloatHandle,
			ID:   13,
			Type: StringType,
		},
		SETEX_COMMAND: {
			Name: SETEX_COMMAND,
			Func: c.SetExHandle,
			ID:   14,
			Type: StringType,
		},
		MGET_COMMAND: {
			Name:     MGET_COMMAND,
			Func:     c.MGetHandle,
			ID:       15,
			Type:     StringType,
			ReadOnly: true,
		},
		MSET_COMMAND: {
			Name: MSET_COMMAND,
			Func: c.MSetHandle,
			ID:   16,
			Type: StringType,
		},
		MSETNX_COMMAND: {
			Name: MSETNX_COMMAND,
			Func: c.MSetNXHandle,
			ID:   17,
			Type: StringType,
		},
		PSETEX_COMMAND: {
			Name: PSETEX_COMMAND,
			Func: c.PSetExHandle,
			ID:   18,
			Type: StringType,
		},
		SETRANGE_COMMAND: {
			Name: SETRANGE_COMMAND,
			Func: c.SetRangeHandle,
			ID:   19,
			Type: StringType,
		},
		BITCOUNT_COMMAND: {
			Name: BITCOUNT_COMMAND,
			Func: c.BitCountHandle,
			ID:   20,
			Type: StringType,
		},
		GETBIT_COMMAND: {
			Name: GETBIT_COMMAND,
			Func: c.GetBitHandle,
			ID:   21,
			Type: StringType,
		},
		SETBIT_COMMAND: {
			Name: SETBIT_COMMAND,
			Func: c.SetBitHandle,
			ID:   22,
			Type: StringType,
		},
		BITPOS_COMMAND: {
			Name: BITPOS_COMMAND,
			Func: c.BitPosHandle,
			ID:   23,
			Type: StringType,
		},
		BITOP_COMMAND: {
			Name: BITOP_COMMAND,
			Func: c.BitOpHandle,
			ID:   24,
			Type: StringType,
		},
		// ------------------- string end 31 ------------------------------------

		// ------------------- hash start 32 ------------------------------------
		HGET_COMMAND: {
			Name:     HGET_COMMAND,
			Func:     c.HGetHandle,
			ReadOnly: true,
			ID:       32,
			Type:     HashType,
		},
		HSET_COMMAND: {
			Name: HSET_COMMAND,
			Func: c.HSetHandle,
			ID:   33,
			Type: HashType,
		},
		HDEL_COMMAND: {
			Name: HDEL_COMMAND,
			Func: c.HDelHandle,
			ID:   34,
			Type: HashType,
		},
		HEXISTS_COMMAND: {
			Name:     HEXISTS_COMMAND,
			Func:     c.HExistsHandle,
			ID:       35,
			ReadOnly: true,
			Type:     HashType,
		},
		HLEN_COMMAND: {
			Name:     HLEN_COMMAND,
			Func:     c.HLenHandle,
			ID:       36,
			ReadOnly: true,
			Type:     HashType,
		},
		HGETALL_COMMAND: {
			Name:     HGETALL_COMMAND,
			Func:     c.HGetAllHandle,
			ID:       37,
			ReadOnly: true,
			Type:     HashType,
		},
		HINCRBY_COMMAND: {
			Name: HINCRBY_COMMAND,
			Func: c.HIncrByHandle,
			ID:   38,
			Type: HashType,
		},
		HINCRBYFLOAT_COMMAND: {
			Name: HINCRBYFLOAT_COMMAND,
			Func: c.HIncrByFloatHandle,
			ID:   39,
			Type: HashType,
		},
		HKEYS_COMMAND: {
			Name:     HKEYS_COMMAND,
			Func:     c.HKeysHandle,
			ID:       40,
			ReadOnly: true,
			Type:     HashType,
		},
		HVALS_COMMAND: {
			Name:     HVALS_COMMAND,
			Func:     c.HValsHandle,
			ID:       41,
			ReadOnly: true,
			Type:     HashType,
		},
		HMGET_COMMAND: {
			Name:     HMGET_COMMAND,
			Func:     c.HMGetHandle,
			ID:       42,
			ReadOnly: true,
			Type:     HashType,
		},
		HMSET_COMMAND: {
			Name: HMSET_COMMAND,
			Func: c.HMSetHandle,
			ID:   43,
			Type: HashType,
		},
		HSCAN_COMMAND: {
			Name:     HSCAN_COMMAND,
			Func:     c.HScanHandle,
			ID:       44,
			ReadOnly: true,
			Type:     HashType,
		},
		HSETNX_COMMAND: {
			Name: HSETNX_COMMAND,
			Func: c.HSetNXHandle,
			ID:   45,
			Type: HashType,
		},
		HSTRLEN_COMMAND: {
			Name:     HSTRLEN_COMMAND,
			Func:     c.HStrLenHandle,
			ID:       46,
			ReadOnly: true,
			Type:     HashType,
		},
		HRANDFIELD_COMMAND: {
			Name:     HRANDFIELD_COMMAND,
			Func:     c.HRandFieldHandle,
			ID:       47,
			ReadOnly: true,
			Type:     HashType,
		},
		// ------------------- hash end 63 ------------------------------------
		// ------------------- list start 64 ------------------------------------
		LINDEX_COMMAND: {
			Name:     LINDEX_COMMAND,
			Func:     c.LIndexHandle,
			ReadOnly: true,
			ID:       64,
			Type:     ListType,
		},
		LINSERT_COMMAND: {
			Name: LINSERT_COMMAND,
			Func: c.LInsertHandle,
			ID:   65,
			Type: ListType,
		},
		LLEN_COMMAND: {
			Name:     LLEN_COMMAND,
			Func:     c.LLenHandle,
			ReadOnly: true,
			ID:       66,
			Type:     ListType,
		},
		LPOP_COMMAND: {
			Name: LPOP_COMMAND,
			Func: c.LPopHandle,
			ID:   67,
			Type: ListType,
		},
		RPOP_COMMAND: {
			Name: RPOP_COMMAND,
			Func: c.RPopHandle,
			ID:   68,
			Type: ListType,
		},
		RPOPLPUSH_COMMAND: {
			Name: RPOPLPUSH_COMMAND,
			Func: c.RPopLPopHandle,
			ID:   69,
			Type: ListType,
		},
		LPUSH_COMMAND: {
			Name: LPUSH_COMMAND,
			Func: c.LPushHandle,
			ID:   70,
			Type: ListType,
		},
		LPUSHX_COMMAND: {
			Name: LPUSHX_COMMAND,
			Func: c.LPushXHandle,
			ID:   71,
			Type: ListType,
		},
		RPUSH_COMMAND: {
			Name: RPUSH_COMMAND,
			Func: c.RPushHandle,
			ID:   72,
			Type: ListType,
		},
		RPUSHX_COMMAND: {
			Name: RPUSHX_COMMAND,
			Func: c.RPushXHandle,
			ID:   73,
			Type: ListType,
		},
		LREM_COMMAND: {
			Name: LREM_COMMAND,
			Func: c.LRemHandle,
			ID:   74,
			Type: ListType,
		},
		LSET_COMMAND: {
			Name: LSET_COMMAND,
			Func: c.LSetHandle,
			ID:   75,
			Type: ListType,
		},
		LTRIM_COMMAND: {
			Name: LTRIM_COMMAND,
			Func: c.LTrimHandle,
			ID:   76,
			Type: ListType,
		},
		LRANGE_COMMAND: {
			Name:     LRANGE_COMMAND,
			Func:     c.LRangeHandle,
			ReadOnly: true,
			ID:       77,
			Type:     ListType,
		},
		LINFO_COMMAND: {
			Name:     LINFO_COMMAND,
			Func:     c.LInfoHandle,
			ReadOnly: true,
			ID:       78,
			Type:     ListType,
		},
		BLMOVE_COMMAND: {
			Name:            BLMOVE_COMMAND,
			Func:            c.BlMoveHandle,
			ID:              79,
			NoSupportScript: true,
			Type:            ListType,
		},
		BLPOP_COMMAND: {
			Name:            BLPOP_COMMAND,
			Func:            c.BlPopHandle,
			ID:              80,
			NoSupportScript: true,
			Type:            ListType,
		},
		BRPOP_COMMAND: {
			Name:            BRPOP_COMMAND,
			Func:            c.BrPopHandle,
			ID:              81,
			NoSupportScript: true,
			Type:            ListType,
		},
		BRPOPLPUSH_COMMAND: {
			Name:            BRPOPLPUSH_COMMAND,
			Func:            c.BRPopLPushHandle,
			ID:              82,
			NoSupportScript: true,
			Type:            ListType,
		},
		LPOS_COMMAND: {
			Name:     LPOS_COMMAND,
			Func:     c.LPosHandle,
			ID:       83,
			Type:     ListType,
			ReadOnly: true,
		},
		LMOVE_COMMAND: {
			Name: LMOVE_COMMAND,
			Func: c.LMoveHandle,
			ID:   84,
			Type: ListType,
		},
		// ------------------- list end 95 ------------------------------------
		// ------------------- set start 96 ------------------------------------
		SADD_COMMAND: {
			Name: SADD_COMMAND,
			Func: c.SAddHandle,
			ID:   96,
			Type: SetType,
		},
		SREM_COMMAND: {
			Name: SREM_COMMAND,
			Func: c.SRemHandle,
			ID:   97,
			Type: SetType,
		},
		SPOP_COMMAND: {
			Name: SPOP_COMMAND,
			Func: c.SPopHandle,
			ID:   98,
			Type: SetType,
		},
		SMOVE_COMMAND: {
			Name: SMOVE_COMMAND,
			Func: c.SMoveHandle,
			ID:   99,
			Type: SetType,
		},
		SCARD_COMMAND: {
			Name:     SCARD_COMMAND,
			Func:     c.SCardHandle,
			ID:       100,
			Type:     SetType,
			ReadOnly: true,
		},
		SISMEMBER_COMMAND: {
			Name:     SISMEMBER_COMMAND,
			Func:     c.SIsMemberHandle,
			ID:       101,
			Type:     SetType,
			ReadOnly: true,
		},
		SINTER_COMMAND: {
			Name:     SINTER_COMMAND,
			Func:     c.SInterHandle,
			ID:       102,
			Type:     SetType,
			ReadOnly: true,
		},
		SINTERSTORE_COMMAND: {
			Name: SINTERSTORE_COMMAND,
			Func: c.SInterStoreHandle,
			ID:   103,
			Type: SetType,
		},
		SUNION_COMMAND: {
			Name:     SUNION_COMMAND,
			Func:     c.SUnionHandle,
			ID:       104,
			Type:     SetType,
			ReadOnly: true,
		},
		SUNIONSTORE_COMMAND: {
			Name: SUNIONSTORE_COMMAND,
			Func: c.SUnionStoreHandle,
			ID:   105,
			Type: SetType,
		},
		SDIFF_COMMAND: {
			Name:     SDIFF_COMMAND,
			Func:     c.SDiffHandle,
			ID:       106,
			Type:     SetType,
			ReadOnly: true,
		},
		SDIFFSTORE_COMMAND: {
			Name: SDIFFSTORE_COMMAND,
			Func: c.SDiffStoreHandle,
			ID:   107,
			Type: SetType,
		},
		SMEMBERS_COMMAND: {
			Name:     SMEMBERS_COMMAND,
			Func:     c.SMembersHandle,
			ID:       108,
			Type:     SetType,
			ReadOnly: true,
		},
		SRANDMEMBER_COMMAND: {
			Name:     SRANDMEMBER_COMMAND,
			Func:     c.SRandMemberHandle,
			ID:       109,
			Type:     SetType,
			ReadOnly: true,
		},
		SSCAN_COMMAND: {
			Name:     SSCAN_COMMAND,
			Func:     c.SScanHandle,
			ID:       110,
			Type:     SetType,
			ReadOnly: true,
		},
		SMISMEMBER_COMMAND: {
			Name:     SMISMEMBER_COMMAND,
			Func:     c.SMIsMemberHandle,
			ID:       111,
			Type:     SetType,
			ReadOnly: true,
		},
		// ------------------- set end 127 ------------------------------------
		// ------------------- sorted set start 128 ---------------------------
		ZADD_COMMAND: {
			Name: ZADD_COMMAND,
			Func: c.ZAddHandle,
			ID:   128,
			Type: ZsetType,
		},
		ZCARD_COMMAND: {
			Name:     ZCARD_COMMAND,
			Func:     c.ZCardHandle,
			ReadOnly: true,
			ID:       129,
			Type:     ZsetType,
		},
		ZCOUNT_COMMAND: {
			Name:     ZCOUNT_COMMAND,
			Func:     c.ZCountHandle,
			ReadOnly: true,
			ID:       130,
			Type:     ZsetType,
		},
		ZRANGEBYLEX_COMMAND: {
			Name:     ZRANGEBYLEX_COMMAND,
			Func:     c.ZRangeByLexHandle,
			ReadOnly: true,
			ID:       131,
			Type:     ZsetType,
		},
		ZRANGEBYSCORE_COMMAND: {
			Name:     ZRANGEBYSCORE_COMMAND,
			Func:     c.ZRangeByScoreHandle,
			ReadOnly: true,
			ID:       132,
			Type:     ZsetType,
		},
		ZDIFFSTORE_COMMAND: {
			Name: ZDIFFSTORE_COMMAND,
			Func: c.ZDiffStoreHandle,
			ID:   133,
			Type: ZsetType,
		},
		ZINTERSTORE_COMMAND: {
			Name: ZINTERSTORE_COMMAND,
			Func: c.ZInterStoreHandle,
			ID:   134,
			Type: ZsetType,
		},
		ZRANK_COMMAND: {
			Name:     ZRANK_COMMAND,
			Func:     c.ZRankHandle,
			ReadOnly: true,
			ID:       135,
			Type:     ZsetType,
		},
		ZREVRANK_COMMAND: {
			Name:     ZREVRANK_COMMAND,
			Func:     c.ZRevRankHandle,
			ReadOnly: true,
			ID:       136,
			Type:     ZsetType,
		},
		ZREMRANGEBYLEX_COMMAND: {
			Name: ZREMRANGEBYLEX_COMMAND,
			Func: c.ZRemRangeByLexHandle,
			ID:   137,
			Type: ZsetType,
		},
		ZREMRANGEBYRANK_COMMAND: {
			Name: ZREMRANGEBYRANK_COMMAND,
			Func: c.ZRemRangeByRankHandle,
			ID:   138,
			Type: ZsetType,
		},
		ZREMRANGEBYSCORE_COMMAND: {
			Name: ZREMRANGEBYSCORE_COMMAND,
			Func: c.ZRemRangeByScoreHandle,
			ID:   139,
			Type: ZsetType,
		},
		ZREVRANGEBYLEX_COMMAND: {
			Name:     ZREVRANGEBYLEX_COMMAND,
			Func:     c.ZRevRangeByLexHandle,
			ReadOnly: true,
			ID:       140,
			Type:     ZsetType,
		},
		ZREVRANGEBYSCORE_COMMAND: {
			Name:     ZREVRANGEBYSCORE_COMMAND,
			Func:     c.ZRevRangeByScoreHandle,
			ReadOnly: true,
			ID:       141,
			Type:     ZsetType,
		},
		ZSCORE_COMMAND: {
			Name:     ZSCORE_COMMAND,
			Func:     c.ZScoreHandle,
			ReadOnly: true,
			ID:       142,
			Type:     ZsetType,
		},
		ZUNIONSTORE_COMMAND: {
			Name: ZUNIONSTORE_COMMAND,
			Func: c.ZUnionStoreHandle,
			ID:   143,
			Type: ZsetType,
		},
		ZLEXCOUNT_COMMAND: {
			Name:     ZLEXCOUNT_COMMAND,
			Func:     c.ZLexCountHandle,
			ReadOnly: true,
			ID:       144,
			Type:     ZsetType,
		},
		ZRANGE_COMMAND: {
			Name:     ZRANGE_COMMAND,
			Func:     c.ZRangeHandle,
			ReadOnly: true,
			ID:       145,
			Type:     ZsetType,
		},
		ZREVRANGE_COMMAND: {
			Name:     ZREVRANGE_COMMAND,
			Func:     c.ZRevRangeHandle,
			ReadOnly: true,
			ID:       146,
			Type:     ZsetType,
		},
		ZMSCORE_COMMAND: {
			Name:     ZMSCORE_COMMAND,
			Func:     c.ZMScoreHandle,
			ReadOnly: true,
			ID:       147,
			Type:     ZsetType,
		},
		ZINCRBY_COMMAND: {
			Name: ZINCRBY_COMMAND,
			Func: c.ZIncrByHandle,
			ID:   148,
			Type: ZsetType,
		},
		ZPOPMAX_COMMAND: {
			Name: ZPOPMAX_COMMAND,
			Func: c.ZPopMaxHandle,
			ID:   149,
			Type: ZsetType,
		},
		ZPOPMIN_COMMAND: {
			Name: ZPOPMIN_COMMAND,
			Func: c.ZPopMinHandle,
			ID:   150,
			Type: ZsetType,
		},
		ZSCAN_COMMAND: {
			Name:     ZSCAN_COMMAND,
			Func:     c.ZScanHandle,
			ReadOnly: true,
			ID:       151,
			Type:     ZsetType,
		},
		ZINTER_COMMAND: {
			Name:     ZINTER_COMMAND,
			Func:     c.ZInterHandle,
			ReadOnly: true,
			ID:       152,
			Type:     ZsetType,
		},
		ZRANGESTORE_COMMAND: {
			Name: ZRANGESTORE_COMMAND,
			Func: c.ZRangeStoreHandle,
			ID:   153,
			Type: ZsetType,
		},
		ZREM_COMMAND: {
			Name: ZREM_COMMAND,
			Func: c.ZRemHandle,
			ID:   154,
			Type: ZsetType,
		},
		ZUNION_COMMAND: {
			Name:     ZUNION_COMMAND,
			Func:     c.ZUnionHandle,
			ReadOnly: true,
			ID:       155,
			Type:     ZsetType,
		},
		ZRANDMEMBER_COMMAND: {
			Name:     ZRANDMEMBER_COMMAND,
			Func:     c.ZRandMemberHandle,
			ReadOnly: true,
			ID:       156,
			Type:     ZsetType,
		},
		ZDIFF_COMMAND: {
			Name:     ZDIFF_COMMAND,
			Func:     c.ZDiffHandle,
			ReadOnly: true,
			ID:       157,
			Type:     ZsetType,
		},
		BZPOPMAX_COMMAND: {
			Name:            BZPOPMAX_COMMAND,
			Func:            c.BZPopMaxHandle,
			ID:              158,
			NoSupportScript: true,
			Type:            ZsetType,
		},
		BZPOPMIN_COMMAND: {
			Name:            BZPOPMIN_COMMAND,
			Func:            c.BZPopMinHandle,
			ID:              159,
			NoSupportScript: true,
			Type:            ZsetType,
		},
		// ------------------- sorted set end 159 ---------------------------
		// ------------------- json start 160 ---------------------------
		JSONSET_COMMAND: {
			Name: JSONSET_COMMAND,
			Func: c.JsonSetHandle,
			ID:   160,
			Type: JsonType,
		},
		JSONGET_COMMAND: {
			Name: JSONGET_COMMAND,
			Func: c.JsonGetHandle,
			ID:   161,
			Type: JsonType,
		},
		JSONDEL_COMMAND: {
			Name: JSONDEL_COMMAND,
			Func: c.JsonDelHandle,
			ID:   162,
			Type: JsonType,
		},
		// ------------------- json end 191 ---------------------------
		// ------------------- script start 192 ---------------------------

		EVAL_COMMAND: {
			Name: EVAL_COMMAND,
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_COMMAND)
			},
			NoSupportScript: true,
			ID:              192,
			Type:            ScriptType,
		},
		EVALSHA_COMMAND: {
			Name: EVALSHA_COMMAND,
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			NoSupportScript: true,
			ID:              193,
			Type:            ScriptType,
		},
		EVAL_RO_COMMAND: {
			Name: EVAL_RO_COMMAND,
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_RO_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
			ID:              194,
			Type:            ScriptType,
		},
		EVALSHA_RO_COMMAND: {
			Name: EVALSHA_RO_COMMAND,
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
			ID:              195,
			Type:            ScriptType,
		},
		SCRIPT_COMMAND: {
			Name:            SCRIPT_COMMAND,
			Func:            c.ScriptHandle,
			NoSupportScript: true,
			ID:              196,
			Type:            ScriptType,
		},
		// ------------------- script end 223 ---------------------------
		// ------------------- keys start 224 ---------------------------
		SCAN_COMMAND: {
			Name:     SCAN_COMMAND,
			Func:     c.ScanHandle,
			ReadOnly: true,
			ID:       224,
			Type:     GeneralType,
		},
		TYPE_COMMAND: {
			Name:     TYPE_COMMAND,
			Func:     c.TypeHandle,
			ReadOnly: true,
			ID:       225,
			Type:     GeneralType,
		},
		KEYS_COMMAND: {
			Name:     KEYS_COMMAND,
			Func:     c.KeysHandle,
			ReadOnly: true,
			ID:       226,
			Type:     GeneralType,
		},
		DEL_COMMAND: {
			Name: DEL_COMMAND,
			Func: c.DELHandle,
			ID:   227,
			Type: GeneralType,
		},
		TTL_COMMAND: {
			Name:     TTL_COMMAND,
			Func:     c.TTLHandle,
			ReadOnly: true,
			ID:       228,
			Type:     GeneralType,
		},
		EXPIRE_COMMAND: {
			Name: EXPIRE_COMMAND,
			Func: c.ExpireHandle,
			ID:   229,
			Type: GeneralType,
		},
		EXPIREAT_COMMAND: {
			Name: EXPIREAT_COMMAND,
			Func: c.ExpireAtHandle,
			ID:   230,
			Type: GeneralType,
		},
		PERSIST_COMMAND: {
			Name: PERSIST_COMMAND,
			Func: c.PersistHandle,
			ID:   231,
			Type: GeneralType,
		},
		PEXPIRE_COMMAND: {
			Name: PEXPIRE_COMMAND,
			Func: c.PExpireHandle,
			ID:   232,
			Type: GeneralType,
		},
		PEXPIREAT_COMMAND: {
			Name: PEXPIREAT_COMMAND,
			Func: c.PExpireAtHandle,
			ID:   233,
			Type: GeneralType,
		},
		PEXPIRETIME_COMMAND: {
			Name:     PEXPIRETIME_COMMAND,
			Func:     c.PExpireTimeHandle,
			ID:       234,
			ReadOnly: true,
			Type:     GeneralType,
		},
		EXPIRETIME_COMMAND: {
			Name:     EXPIRETIME_COMMAND,
			Func:     c.ExpireTimeHandle,
			ReadOnly: true,
			ID:       235,
			Type:     GeneralType,
		},
		PTTL_COMMAND: {
			Name:     PTTL_COMMAND,
			Func:     c.PTTLHandle,
			ReadOnly: true,
			ID:       236,
			Type:     GeneralType,
		},
		UNLINK_COMMAND: {
			Name: UNLINK_COMMAND,
			Func: c.UnlinkHandle,
			ID:   237,
			Type: GeneralType,
		},
		EXISTS_COMMAND: {
			Name:     EXISTS_COMMAND,
			Func:     c.ExistsHandle,
			ReadOnly: true,
			ID:       238,
			Type:     GeneralType,
		},
		ALL_COMMAND: {
			Name:     ALL_COMMAND,
			Func:     c.AllHandle,
			ReadOnly: true,
			ID:       239,
			Type:     GeneralType,
		},
		OBJECT_COMMAND: {
			Name:     OBJECT_COMMAND,
			Func:     c.ObjectHandle,
			ReadOnly: true,
			ID:       240,
			Type:     GeneralType,
		},
		TOUCH_COMMAND: {
			Name:     TOUCH_COMMAND,
			Func:     c.TouchHandle,
			ID:       241,
			Type:     UnknownType,
			ReadOnly: true,
		},
		// ------------------- keys end 255 ---------------------------

		// ------------------- server start 256 ---------------------------
		FLUSHALL_COMMAND: {
			Name: FLUSHALL_COMMAND,
			Func: c.FlushAllHandle,
			ID:   256,
			Type: ServerType,
		},
		FLUSHDB_COMMAND: {
			Name: FLUSHDB_COMMAND,
			Func: c.FlushDBHandle,
			ID:   257,
			Type: ServerType,
		},
		INFO_COMMAND: {
			Func:     c.InfoHandle,
			ReadOnly: true,
			ID:       133,
			Type:     ServerType,
		},
		TIME_COMMAND: {
			Func:     c.TimeHandle,
			ReadOnly: true,
			ID:       137,
			Type:     ServerType,
		},
		ACL_COMMAND: {
			Name: ACL_COMMAND,
			Func: c.AclHandle,
			ID:   67,
			Type: UserType,
		},
		CONFIG_COMMAND: {
			Func: c.ConfigHandle,
			ID:   68,
			Type: ServerType,
		},
		DBSIZE_COMMAND: {
			Func:     c.DBSizeHandle,
			ReadOnly: true,
			ID:       502,
			Type:     ServerType,
		},
		// ------------------- server end 287 ---------------------------
		// ------------------- client start 288 ---------------------------
		ECHO_COMMAND: {
			Func:     c.EchoHandle,
			ReadOnly: true,
			ID:       135,
			Type:     ClientType,
		},
		PING_COMMAND: {
			Func:     c.PingHandle,
			ReadOnly: true,
			ID:       136,
			Type:     ClientType,
		},
		SELECT_COMMAND: {
			Func: c.SelectHandle,
			ID:   128,
			Type: ClientType,
		},
		CLIENT_COMMAND: {
			Func:     c.ClientHandle,
			ReadOnly: true,
			ID:       134,
			Type:     ClientType,
		},
		AUTH_COMMAND: {
			Func:            c.AuthHandle,
			ID:              127,
			NoSupportScript: true,
			Type:            UserType,
		},
		// ------------------- client end 319 ---------------------------
		// ------------------- stream start 320 ---------------------------
		XADD_COMMAND: {
			Func: c.XADDHandle,
			ID:   320,
			Type: StreamType,
		},
		XRANGE_COMMAND: {
			Func:     c.XRangeHandle,
			ReadOnly: true,
			ID:       321,
			Type:     StreamType,
		},
		// ------------------- stream end 351 ---------------------------
		// ------------------- pubsub start 352 ---------------------------
		PUBSUB_COMMAND: {
			Func: c.PubSubHandle,
			ID:   356,
			Type: PubSubType,
		},
		PUBLISH_COMMAND: {
			Func: c.PublishHandle,
			ID:   357,
			Type: PubSubType,
		},
		// ------------------- pubsub end 383 ---------------------------

		// ------------------- translate start 992 ---------------------------
		UNWATCH_COMMAND: {
			Func: c.UnWatchHandle,
			ID:   992,
			Type: UnknownType,
		},
	}
	return c
}
