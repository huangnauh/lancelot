package store

import (
	"bytes"
	"context"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	tikvConfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/utils"
)

type Client struct {
	store   kv.Storage
	etcd    *clientv3.Client
	manager *Manager
	conf    *config.Store
	mock    bool
	uuid    string
	ctx     context.Context
	cancel  context.CancelFunc
}

func Open(c *config.Config) (*Client, error) {
	conf := &c.Store
	u, err := url.Parse(conf.Path)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &Client{
		conf:   conf,
		uuid:   conf.UUID,
		ctx:    ctx,
		cancel: cancel,
	}

	if strings.EqualFold(u.Scheme, "mocktikv") {
		var driver mockstore.MockTiKVDriver
		s, err := driver.Open(conf.Path)
		if err != nil {
			utils.ZapLog.Error("mocktikv driver open", zap.Error(err))
			return nil, err
		}
		client.store = s
		client.mock = true
		return client, nil
	}

	driver := tikv.Driver{}
	cfg := tikvConfig.GetGlobalConfig()
	cfg.Log.Level = conf.Level
	cfg.Log.EnableSlowLog = false
	tikvConfig.StoreGlobalConfig(cfg)
	s, err := driver.Open(conf.Path)
	if err != nil {
		utils.ZapLog.Error("tikv driver open", zap.Error(err))
		return nil, err
	}

	client.store = s
	if ebd, ok := s.(tikv.EtcdBackend); ok {
		var addrs []string
		var err error
		if addrs, err = ebd.EtcdAddrs(); err != nil {
			return nil, err
		}
		if addrs != nil {
			etcdLogCfg := zap.NewProductionConfig()
			etcdLogCfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
			cli, err := clientv3.New(clientv3.Config{
				LogConfig:        &etcdLogCfg,
				Endpoints:        addrs,
				AutoSyncInterval: 30 * time.Second,
				DialTimeout:      5 * time.Second,
				DialOptions: []grpc.DialOption{
					grpc.WithBackoffMaxDelay(time.Second * 3),
					grpc.WithKeepaliveParams(keepalive.ClientParameters{
						Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
						Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
					}),
				},
				TLS: ebd.TLSConfig(),
			})
			if err != nil {
				return nil, err
			}
			client.etcd = cli

			client.manager = NewManager(cli, client.uuid)
			err = client.manager.RunElection()
			if err != nil {
				return nil, err
			}
		}
	}

	if client.conf.GCEnable {
		go client.RunGC()
	}
	return client, nil
}

func (c *Client) NewTxn() *Txn {
	return &Txn{client: c}
}

func (c *Client) ID() string {
	return c.uuid
}

func (c *Client) GetLeader(ctx context.Context) string {
	if c.manager == nil {
		return ""
	}
	return c.manager.GetLeader(ctx)
}

func (c *Client) GetEtcdCtl() *clientv3.Client {
	return c.etcd
}

func (c *Client) Close() {
	c.store.Close()
	if c.etcd != nil {
		c.etcd.Close()
		c.manager.Cancel()
	}
	c.cancel()
}

func (c *Client) CurrentVersion() (uint64, error) {
	ver, err := c.store.CurrentVersion(oracle.GlobalTxnScope)
	if err != nil {
		return 0, err
	}
	return ver.Ver, nil
}

func (c *Client) GetSafePointKV() tikv.SafePointKV {
	store := c.store.(tikv.Storage)
	return store.GetSafePointKV()
}

func (c *Client) LoadTS(savedPath string) (uint64, error) {
	kv := c.GetSafePointKV()

	str, err := kv.Get(savedPath)
	if err != nil {
		return 0, err
	}

	if str == "" {
		return 0, nil
	}

	t, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, err
	}
	return t, nil
}

func (c *Client) SaveTS(savedPath string, t uint64) error {
	utils.ZapLog.Debug("save ts", zap.String("path", savedPath), zap.Uint64("ts", t))
	kv := c.GetSafePointKV()
	s := strconv.FormatUint(t, 10)
	err := kv.Put(savedPath, s)
	if err != nil {
		return err
	}
	return nil
}

type KVCallback func(key, value []byte) bool

func (c *Client) List(start, end []byte, limit int, callback KVCallback) error {
	txn := c.NewTxn()
	err := txn.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	err = txn.List(start, end, limit, callback)
	return err
}

type ClientCallback func(c *Client)

func (c *Client) DelteRange(start, end []byte, callback ClientCallback) error {
	cur := start
	var count int
	var err error
	for bytes.Compare(cur, start) >= 0 && bytes.Compare(cur, end) < 0 {
		cur, count, err = c.DeleteUntil(cur, end, c.conf.BatchLimit, nil)
		if err != nil {
			return err
		}
		if count < c.conf.BatchLimit {
			break
		}
	}
	if callback != nil {
		callback(c)
	}
	return nil
}

func (c *Client) DeleteRangeUntil(start, end []byte, callback KVCallback) error {
	cur := start
	var count int
	var err error
	for bytes.Compare(cur, start) >= 0 && bytes.Compare(cur, end) < 0 {
		cur, count, err = c.DeleteUntil(cur, end, c.conf.BatchLimit, callback)
		if err != nil {
			return err
		}
		if count < c.conf.BatchLimit {
			break
		}
	}
	return nil
}

func (c *Client) DeleteUntil(start, end []byte, limit int, callback KVCallback) ([]byte, int, error) {
	txn := c.NewTxn()
	err := txn.Begin()
	if err != nil {
		return start, 0, err
	}
	defer txn.Rollback()
	it, err := txn.Iter(start, end, false)
	if err != nil {
		return start, 0, err
	}
	defer it.Close()
	cur, count, err := it.DeleteUntil(limit, callback)
	if err != nil {
		return cur, count, err
	}
	err = txn.Commit()
	return cur, count, err
}

func (c *Client) Delete(key []byte) error {
	txn := c.NewTxn()
	err := txn.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()
	err = txn.Del(key)
	if err != nil {
		return err
	}
	if err = txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (c *Client) UnsafeDeleteRange(ctx context.Context, startKey, endKey []byte, concurrency int) error {
	if c.mock {
		return c.DelteRange(startKey, endKey, nil)
	}

	storage := c.store.(tikv.Storage)
	stores, err := storage.GetRegionCache().PDClient().GetAllStores(ctx)
	if err != nil {
		return err
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	})
	tikvCli := storage.GetTiKVClient()

	var wg sync.WaitGroup
	failed := false
	for _, s := range stores {
		if s.State != metapb.StoreState_Up {
			continue
		}

		address := s.Address
		storeID := s.Id
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err := tikvCli.SendRequest(ctx, address, req, tikv.UnsafeDestroyRangeTimeout)
			if err != nil {
				failed = true
				utils.ZapLog.Error("unsafe destroy range store",
					zap.Uint64("store", storeID), zap.Error(err))
				return
			}
			if resp == nil || resp.Resp == nil {
				failed = true
				utils.ZapLog.Error("unsafe destroy range returns nil response from store",
					zap.Uint64("store", storeID))
				return
			}

			errStr := (resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)).Error
			if len(errStr) > 0 {
				failed = true
				utils.ZapLog.Error("unsafe destroy range failed on store",
					zap.Uint64("store", storeID), zap.Error(err))
				return
			}
		}()
	}

	wg.Wait()

	if failed {
		utils.ZapLog.Error("unsafe destroy range failed")
		return UnsafeDestroyRangeFailed
	}

	// Notify all affected regions in the range that UnsafeDestroyRange occurs.
	notifyTask := tikv.NewNotifyDeleteRangeTask(storage, startKey, endKey, concurrency)
	err = notifyTask.Execute(ctx)
	if err != nil {
		utils.ZapLog.Error("failed notifying regions affected by UnsafeDestroyRange",
			zap.Binary("start", startKey), zap.Binary("end", endKey), zap.Error(err))
		return UnsafeDestroyRangeFailed
	}
	return nil
}
