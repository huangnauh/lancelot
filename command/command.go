package command

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coocood/freecache"
	"github.com/tikv/client-go/v2/oracle"
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

func NewCommand(cfg *config.Config) *Command {
	c := &Command{
		cfg:     cfg,
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
		DISCARD_COMMAND: {
			Func: c.discard,
			ID:   509,
		},
		SUBSCRIBE_COMMAND: {
			Func: c.SubscribeHandle,
			ID:   503,
		},
		PSUBSCRIBE_COMMAND: {
			Func: c.PSubscribeHandle,
			ID:   502,
		},
		UNSUBSCRIBE_COMMAND: {
			Func: c.UnsubscribeHandle,
			ID:   501,
		},
		PUNSUBSCRIBE_COMMAND: {
			Func: c.PUnsubscribeHandle,
			ID:   500,
		},
	}

	c.TxnHandle = map[string]TxnHandler{
		GET_COMMAND: {
			Func:     c.GetHandle,
			ReadOnly: true,
			ID:       0,
			Type:     StringType,
		},
		SET_COMMAND: {
			Func: c.SetHandle,
			ID:   1,
			Type: StringType,
		},
		GETSET_COMMAND: {
			Func: c.GetSetHandle,
			ID:   2,
			Type: StringType,
		},
		SETNX_COMMAND: {
			Func: c.SetNXHandle,
			ID:   3,
			Type: StringType,
		},
		STRLEN_COMMAND: {
			Func:     c.StrLenHandle,
			ID:       4,
			ReadOnly: true,
			Type:     StringType,
		},
		APPEND_COMMAND: {
			Func: c.AppendHandle,
			ID:   5,
			Type: StringType,
		},
		DECR_COMMAND: {
			Func: c.DecrHandle,
			ID:   6,
			Type: StringType,
		},
		DECRBY_COMMAND: {
			Func: c.DecrByHandle,
			ID:   7,
			Type: StringType,
		},
		INCR_COMMAND: {
			Func: c.IncrHandle,
			ID:   8,
			Type: StringType,
		},
		INCRBY_COMMAND: {
			Func: c.IncrByHandle,
			ID:   9,
			Type: StringType,
		},
		GETDEL_COMMAND: {
			Func: c.GetDelHandle,
			ID:   10,
			Type: StringType,
		},
		GETEX_COMMAND: {
			Func: c.GetExHandle,
			ID:   11,
			Type: StringType,
		},
		GETRANGE_COMMAND: {
			Func:     c.GetRangeHandle,
			ID:       12,
			ReadOnly: true,
			Type:     StringType,
		},
		INCRBYFLOAT_COMMAND: {
			Func: c.IncrByFloatHandle,
			ID:   13,
			Type: StringType,
		},
		SETEX_COMMAND: {
			Func: c.SetExHandle,
			ID:   14,
			Type: StringType,
		},
		MGET_COMMAND: {
			Func:     c.MGetHandle,
			ID:       15,
			Type:     StringType,
			ReadOnly: true,
		},
		MSET_COMMAND: {
			Func: c.MSetHandle,
			ID:   16,
			Type: StringType,
		},
		MSETNX_COMMAND: {
			Func: c.MSetNXHandle,
			ID:   17,
			Type: StringType,
		},
		PSETEX_COMMAND: {
			Func: c.PSetExHandle,
			ID:   18,
			Type: StringType,
		},
		SETRANGE_COMMAND: {
			Func: c.SetRangeHandle,
			ID:   19,
			Type: StringType,
		},
		BITCOUNT_COMMAND: {
			Func: c.BitCountHandle,
			ID:   20,
			Type: StringType,
		},
		GETBIT_COMMAND: {
			Func: c.GetBitHandle,
			ID:   21,
			Type: StringType,
		},
		SETBIT_COMMAND: {
			Func: c.SetBitHandle,
			ID:   22,
			Type: StringType,
		},
		BITPOS_COMMAND: {
			Func: c.BitPosHandle,
			ID:   23,
			Type: StringType,
		},
		BITOP_COMMAND: {
			Func: c.BitOpHandle,
			ID:   24,
			Type: StringType,
		},
		SADD_COMMAND: {
			Func: c.SAddHandle,
			ID:   25,
			Type: SetType,
		},
		SREM_COMMAND: {
			Func: c.SRemHandle,
			ID:   26,
			Type: SetType,
		},
		SPOP_COMMAND: {
			Func: c.SPopHandle,
			ID:   27,
			Type: SetType,
		},
		SMOVE_COMMAND: {
			Func: c.SMoveHandle,
			ID:   28,
			Type: SetType,
		},
		SCARD_COMMAND: {
			Func:     c.SCardHandle,
			ID:       29,
			Type:     SetType,
			ReadOnly: true,
		},
		SISMEMBER_COMMAND: {
			Func:     c.SIsMemberHandle,
			ID:       30,
			Type:     SetType,
			ReadOnly: true,
		},
		SINTER_COMMAND: {
			Func:     c.SInterHandle,
			ID:       31,
			Type:     SetType,
			ReadOnly: true,
		},
		SINTERSTORE_COMMAND: {
			Func: c.SInterStoreHandle,
			ID:   32,
			Type: SetType,
		},
		SUNION_COMMAND: {
			Func:     c.SUnionHandle,
			ID:       33,
			Type:     SetType,
			ReadOnly: true,
		},
		SUNIONSTORE_COMMAND: {
			Func: c.SUnionStoreHandle,
			ID:   34,
			Type: SetType,
		},
		SDIFF_COMMAND: {
			Func:     c.SDiffHandle,
			ID:       35,
			Type:     SetType,
			ReadOnly: true,
		},
		SDIFFSTORE_COMMAND: {
			Func: c.SDiffStoreHandle,
			ID:   36,
			Type: SetType,
		},
		SMEMBERS_COMMAND: {
			Func:     c.SMembersHandle,
			ID:       37,
			Type:     SetType,
			ReadOnly: true,
		},
		SRANDMEMBER_COMMAND: {
			Func:     c.SRandMemberHandle,
			ID:       38,
			Type:     SetType,
			ReadOnly: true,
		},
		SSCAN_COMMAND: {
			Func:     c.SScanHandle,
			ID:       39,
			Type:     SetType,
			ReadOnly: true,
		},
		SMISMEMBER_COMMAND: {
			Func:     c.SMIsMemberHandle,
			ID:       40,
			Type:     SetType,
			ReadOnly: true,
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
		HLEN_COMMAND: {
			Func:     c.HLenHandle,
			ID:       52,
			ReadOnly: true,
			Type:     HashType,
		},
		HGETALL_COMMAND: {
			Func:     c.HGetAllHandle,
			ID:       53,
			ReadOnly: true,
			Type:     HashType,
		},
		HINCRBY_COMMAND: {
			Func: c.HIncrByHandle,
			ID:   54,
			Type: HashType,
		},
		HINCRBYFLOAT_COMMAND: {
			Func: c.HIncrByFloatHandle,
			ID:   55,
			Type: HashType,
		},
		HKEYS_COMMAND: {
			Func:     c.HKeysHandle,
			ID:       56,
			ReadOnly: true,
			Type:     HashType,
		},
		HVALS_COMMAND: {
			Func:     c.HValsHandle,
			ID:       57,
			ReadOnly: true,
			Type:     HashType,
		},
		HMGET_COMMAND: {
			Func:     c.HMGetHandle,
			ID:       58,
			ReadOnly: true,
			Type:     HashType,
		},
		HMSET_COMMAND: {
			Func: c.HMSetHandle,
			ID:   59,
			Type: HashType,
		},
		HSCAN_COMMAND: {
			Func:     c.HScanHandle,
			ID:       60,
			ReadOnly: true,
			Type:     HashType,
		},
		HSETNX_COMMAND: {
			Func: c.HSetNXHandle,
			ID:   61,
			Type: HashType,
		},
		HSTRLEN_COMMAND: {
			Func:     c.HStrLenHandle,
			ID:       62,
			ReadOnly: true,
			Type:     HashType,
		},
		HRANDFIELD_COMMAND: {
			Func:     c.HRandFieldHandle,
			ID:       63,
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
			Type:     UnknownType,
		},
		ACL_COMMAND: {
			Func: c.AclHandle,
			ID:   67,
			Type: UserType,
		},
		CONFIG_COMMAND: {
			Func: c.ConfigHandle,
			ID:   68,
			Type: UnknownType,
		},
		LINDEX_COMMAND: {
			Func:     c.LIndexHandle,
			ReadOnly: true,
			ID:       80,
			Type:     ListType,
		},
		LINSERT_COMMAND: {
			Func: c.LInsertHandle,
			ID:   81,
			Type: ListType,
		},
		LLEN_COMMAND: {
			Func:     c.LLenHandle,
			ReadOnly: true,
			ID:       82,
			Type:     ListType,
		},
		LPOP_COMMAND: {
			Func: c.LPopHandle,
			ID:   83,
			Type: ListType,
		},
		RPOP_COMMAND: {
			Func: c.RPopHandle,
			ID:   84,
			Type: ListType,
		},
		RPOPLPUSH_COMMAND: {
			Func: c.RPopLPopHandle,
			ID:   85,
			Type: ListType,
		},
		LPUSH_COMMAND: {
			Func: c.LPushHandle,
			ID:   86,
			Type: ListType,
		},
		LPUSHX_COMMAND: {
			Func: c.LPushXHandle,
			ID:   87,
			Type: ListType,
		},
		RPUSH_COMMAND: {
			Func: c.RPushHandle,
			ID:   88,
			Type: ListType,
		},
		RPUSHX_COMMAND: {
			Func: c.RPushXHandle,
			ID:   89,
			Type: ListType,
		},
		LREM_COMMAND: {
			Func: c.LRemHandle,
			ID:   90,
			Type: ListType,
		},
		LSET_COMMAND: {
			Func: c.LSetHandle,
			ID:   91,
			Type: ListType,
		},
		LTRIM_COMMAND: {
			Func: c.LTrimHandle,
			ID:   92,
			Type: ListType,
		},
		LRANGE_COMMAND: {
			Func:     c.LRangeHandle,
			ReadOnly: true,
			ID:       93,
			Type:     ListType,
		},
		LINFO_COMMAND: {
			Func:     c.LInfoHandle,
			ReadOnly: true,
			ID:       94,
			Type:     ListType,
		},
		BLMOVE_COMMAND: {
			Func:            c.BlMoveHandle,
			ID:              95,
			NoSupportScript: true,
			Type:            ListType,
		},
		BLPOP_COMMAND: {
			Func:            c.BlPopHandle,
			ID:              96,
			NoSupportScript: true,
			Type:            ListType,
		},
		BRPOP_COMMAND: {
			Func:            c.BrPopHandle,
			ID:              97,
			NoSupportScript: true,
			Type:            ListType,
		},
		BRPOPLPUSH_COMMAND: {
			Func:            c.BRPopLPushHandle,
			ID:              98,
			NoSupportScript: true,
			Type:            ListType,
		},
		LPOS_COMMAND: {
			Func:     c.LPosHandle,
			ID:       99,
			Type:     ListType,
			ReadOnly: true,
		},
		LMOVE_COMMAND: {
			Func: c.LMoveHandle,
			ID:   100,
			Type: ListType,
		},
		ZADD_COMMAND: {
			Func: c.ZAddHandle,
			ID:   101,
			Type: ZsetType,
		},
		ZCARD_COMMAND: {
			Func:     c.ZCardHandle,
			ReadOnly: true,
			ID:       102,
			Type:     ZsetType,
		},
		ZCOUNT_COMMAND: {
			Func:     c.ZCountHandle,
			ReadOnly: true,
			ID:       103,
			Type:     ZsetType,
		},
		ZRANGEBYLEX_COMMAND: {
			Func:     c.ZRangeByLexHandle,
			ReadOnly: true,
			ID:       104,
			Type:     ZsetType,
		},
		ZRANGEBYSCORE_COMMAND: {
			Func:     c.ZRangeByScoreHandle,
			ReadOnly: true,
			ID:       105,
			Type:     ZsetType,
		},
		ZDIFFSTORE_COMMAND: {
			Func: c.ZDiffStoreHandle,
			ID:   106,
			Type: ZsetType,
		},
		ZINTERSTORE_COMMAND: {
			Func: c.ZInterStoreHandle,
			ID:   107,
			Type: ZsetType,
		},
		ZRANK_COMMAND: {
			Func:     c.ZRankHandle,
			ReadOnly: true,
			ID:       108,
			Type:     ZsetType,
		},
		ZREVRANK_COMMAND: {
			Func:     c.ZRevRankHandle,
			ReadOnly: true,
			ID:       109,
			Type:     ZsetType,
		},
		ZREMRANGEBYLEX_COMMAND: {
			Func: c.ZRemRangeByLexHandle,
			ID:   110,
			Type: ZsetType,
		},
		ZREMRANGEBYRANK_COMMAND: {
			Func: c.ZRemRangeByRankHandle,
			ID:   111,
			Type: ZsetType,
		},
		ZREMRANGEBYSCORE_COMMAND: {
			Func: c.ZRemRangeByScoreHandle,
			ID:   112,
			Type: ZsetType,
		},
		ZREVRANGEBYLEX_COMMAND: {
			Func:     c.ZRevRangeByLexHandle,
			ReadOnly: true,
			ID:       113,
			Type:     ZsetType,
		},
		ZREVRANGEBYSCORE_COMMAND: {
			Func:     c.ZRevRangeByScoreHandle,
			ReadOnly: true,
			ID:       114,
			Type:     ZsetType,
		},
		ZSCORE_COMMAND: {
			Func:     c.ZScoreHandle,
			ReadOnly: true,
			ID:       115,
			Type:     ZsetType,
		},
		ZUNIONSTORE_COMMAND: {
			Func: c.ZUnionStoreHandle,
			ID:   116,
			Type: ZsetType,
		},
		ZLEXCOUNT_COMMAND: {
			Func:     c.ZLexCountHandle,
			ReadOnly: true,
			ID:       117,
			Type:     ZsetType,
		},
		ZRANGE_COMMAND: {
			Func:     c.ZRangeHandle,
			ReadOnly: true,
			ID:       118,
			Type:     ZsetType,
		},
		ZREVRANGE_COMMAND: {
			Func:     c.ZRevRangeHandle,
			ReadOnly: true,
			ID:       119,
			Type:     ZsetType,
		},
		ZMSCORE_COMMAND: {
			Func:     c.ZMScoreHandle,
			ReadOnly: true,
			ID:       120,
			Type:     ZsetType,
		},
		ZINCRBY_COMMAND: {
			Func: c.ZIncrByHandle,
			ID:   121,
			Type: ZsetType,
		},
		ZPOPMAX_COMMAND: {
			Func: c.ZPopMaxHandle,
			ID:   122,
			Type: ZsetType,
		},
		ZPOPMIN_COMMAND: {
			Func: c.ZPopMinHandle,
			ID:   123,
			Type: ZsetType,
		},
		ZSCAN_COMMAND: {
			Func:     c.ZScanHandle,
			ReadOnly: true,
			ID:       124,
			Type:     ZsetType,
		},
		ZINTER_COMMAND: {
			Func:     c.ZInterHandle,
			ReadOnly: true,
			ID:       125,
			Type:     ZsetType,
		},
		ZRANGESTORE_COMMAND: {
			Func: c.ZRangeStoreHandle,
			ID:   126,
			Type: ZsetType,
		},
		ZREM_COMMAND: {
			Func: c.ZRemHandle,
			ID:   127,
			Type: ZsetType,
		},
		ZUNION_COMMAND: {
			Func:     c.ZUnionHandle,
			ReadOnly: true,
			ID:       128,
			Type:     ZsetType,
		},
		ZRANDMEMBER_COMMAND: {
			Func:     c.ZRandMemberHandle,
			ReadOnly: true,
			ID:       129,
			Type:     ZsetType,
		},
		ZDIFF_COMMAND: {
			Func:     c.ZDiffHandle,
			ReadOnly: true,
			ID:       130,
			Type:     ZsetType,
		},
		BZPOPMAX_COMMAND: {
			Func:            c.BZPopMaxHandle,
			ID:              131,
			NoSupportScript: true,
			Type:            ZsetType,
		},
		BZPOPMIN_COMMAND: {
			Func:            c.BZPopMinHandle,
			ID:              132,
			NoSupportScript: true,
			Type:            ZsetType,
		},
		INFO_COMMAND: {
			Func:     c.InfoHandle,
			ReadOnly: true,
			ID:       133,
			Type:     UnknownType,
		},
		CLIENT_COMMAND: {
			Func:     c.ClientHandle,
			ReadOnly: true,
			ID:       134,
			Type:     UnknownType,
		},
		ECHO_COMMAND: {
			Func:     c.EchoHandle,
			ReadOnly: true,
			ID:       135,
			Type:     UnknownType,
		},
		PING_COMMAND: {
			Func:     c.PingHandle,
			ReadOnly: true,
			ID:       136,
			Type:     UnknownType,
		},
		TIME_COMMAND: {
			Func:     c.TimeHandle,
			ReadOnly: true,
			ID:       137,
			Type:     UnknownType,
		},
		TYPE_COMMAND: {
			Func:     c.TypeHandle,
			ReadOnly: true,
			ID:       151,
			Type:     UnknownType,
		},
		KEYS_COMMAND: {
			Func:     c.KeysHandle,
			ReadOnly: true,
			ID:       152,
			Type:     UnknownType,
		},
		DEL_COMMAND: {
			Func: c.DELHandle,
			ID:   153,
			Type: UnknownType,
		},
		TTL_COMMAND: {
			Func:     c.TTLHandle,
			ReadOnly: true,
			ID:       154,
			Type:     UnknownType,
		},
		EXPIRE_COMMAND: {
			Func: c.ExpireHandle,
			ID:   155,
			Type: UnknownType,
		},
		EXPIREAT_COMMAND: {
			Func: c.ExpireAtHandle,
			ID:   156,
			Type: UnknownType,
		},
		PERSIST_COMMAND: {
			Func: c.PersistHandle,
			ID:   157,
			Type: UnknownType,
		},
		PEXPIRE_COMMAND: {
			Func: c.PExpireHandle,
			ID:   158,
			Type: UnknownType,
		},
		PEXPIREAT_COMMAND: {
			Func: c.PExpireAtHandle,
			ID:   159,
			Type: UnknownType,
		},
		PEXPIRETIME_COMMAND: {
			Func: c.PExpireTimeHandle,
			ID:   160,
			Type: UnknownType,
		},
		EXPIRETIME_COMMAND: {
			Func: c.ExpireTimeHandle,
			ID:   161,
			Type: UnknownType,
		},
		TOUCH_COMMAND: {
			Func:     c.TouchHandle,
			ID:       162,
			Type:     UnknownType,
			ReadOnly: true,
		},
		PTTL_COMMAND: {
			Func:     c.PTTLHandle,
			ReadOnly: true,
			ID:       163,
			Type:     UnknownType,
		},
		UNLINK_COMMAND: {
			Func: c.UnlinkHandle,
			ID:   164,
			Type: UnknownType,
		},
		EXISTS_COMMAND: {
			Func:     c.ExistsHandle,
			ReadOnly: true,
			ID:       104,
			Type:     UnknownType,
		},
		ALL_COMMAND: {
			Func: c.AllHandle,
			ID:   126,
			Type: UnknownType,
		},
		AUTH_COMMAND: {
			Func:            c.AuthHandle,
			ID:              127,
			NoSupportScript: true,
			Type:            UserType,
		},
		SELECT_COMMAND: {
			Func: c.SelectHandle,
			ID:   128,
			Type: UnknownType,
		},
		XADD_COMMAND: {
			Func: c.XADDHandle,
			ID:   480,
			Type: StreamType,
		},
		XRANGE_COMMAND: {
			Func:     c.XRangeHandle,
			ReadOnly: true,
			ID:       481,
			Type:     StreamType,
		},
		PUBSUB_COMMAND: {
			Func: c.PubSubHandle,
			ID:   499,
			Type: StreamType,
		},
		PUBLISH_COMMAND: {
			Func: c.PublishHandle,
			ID:   500,
			Type: StreamType,
		},
		UNWATCH_COMMAND: {
			Func: c.UnWatchHandle,
			ID:   501,
			Type: UnknownType,
		},
		DBSIZE_COMMAND: {
			Func:     c.DBSizeHandle,
			ReadOnly: true,
			ID:       502,
			Type:     UnknownType,
		},
		OBJECT_COMMAND: {
			Func:     c.ObjectHandle,
			ReadOnly: true,
			ID:       503,
			Type:     UnknownType,
		},
		EVAL_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_COMMAND)
			},
			NoSupportScript: true,
			ID:              508,
			Type:            UnknownType,
		},
		EVALSHA_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			NoSupportScript: true,
			ID:              509,
			Type:            UnknownType,
		},
		EVAL_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVAL_RO_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
			ID:              510,
			Type:            UnknownType,
		},
		EVALSHA_RO_COMMAND: {
			Func: func(txn *store.Txn, args [][]byte) interface{} {
				return c.evalHandle(txn, args, EVALSHA_COMMAND)
			},
			ReadOnly:        true,
			NoSupportScript: true,
			ID:              511,
			Type:            UnknownType,
		},
		SCRIPT_COMMAND: {
			Func:            c.ScriptHandle,
			NoSupportScript: true,
			ID:              512,
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
	id, err := c.GetCurrentID()
	if err != nil {
		id = oracle.GoTimeToTS(time.Now())
	}
	conn.ID = id
	return true
}

func (c *Command) Close(conn *redcon.Conn, err error) {
	utils.ZapLog.Debug("Close", zap.String("remote", conn.RemoteAddr()), zap.Error(err))
	atomic.AddInt64(&c.Info.ConnectedClients, -1)
	connTxn := conn.Transaction()
	if connTxn != nil {
		txn, ok := connTxn.(*store.Txn)
		if ok {
			txn.Close()
		}
	}
}
