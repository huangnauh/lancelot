package store

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	tikvConfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/utils"
)

type Client struct {
	store   *tikv.KVStore
	etcd    *clientv3.Client
	manager *Manager
	conf    *config.Store
	uuid    string
	ctx     context.Context
	cancel  context.CancelFunc
}

func Open(conf *config.Store) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())

	client := &Client{
		conf:   conf,
		uuid:   conf.UUID,
		ctx:    ctx,
		cancel: cancel,
	}

	var err error
	if conf.PDAddrs == nil {
		c, pdClient, cluster, err := unistore.New("")
		if err != nil {
			return nil, err
		}
		unistore.BootstrapWithSingleStore(cluster)
		client.store, err = tikv.NewTestTiKVStore(c, pdClient, nil, nil, 0)
	} else {
		client.store, err = tikv.NewTxnClient(conf.PDAddrs)
	}
	if err != nil {
		utils.ZapLog.Error("new tikv client", zap.Error(err))
		return nil, err
	}

	if len(conf.PDAddrs) > 0 {
		cfg := tikvConfig.GetGlobalConfig()
		etcdLogCfg := zap.NewProductionConfig()
		etcdLogCfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
		cli, err := clientv3.New(clientv3.Config{
			LogConfig:        &etcdLogCfg,
			Endpoints:        conf.PDAddrs,
			AutoSyncInterval: 30 * time.Second,
			DialTimeout:      5 * time.Second,
			DialOptions: []grpc.DialOption{
				grpc.WithBackoffMaxDelay(time.Second * 3),
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
					Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
				}),
			},
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
		return c.ID()
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
	return c.store.CurrentTimestamp(oracle.GlobalTxnScope)
}

func (c *Client) GetSafePointKV() tikv.SafePointKV {
	return c.store.GetSafePointKV()
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
	utils.ZapLog.Debug("load ts", zap.String("path", savedPath), zap.Uint64("ts", t))
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
	if c.conf.PDAddrs == nil {
		return c.DelteRange(startKey, endKey, nil)
	}

	stores, err := c.store.GetRegionCache().PDClient().GetAllStores(ctx)
	if err != nil {
		return err
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	})
	tikvCli := c.store.GetTiKVClient()

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

			resp, err := tikvCli.SendRequest(ctx, address, req, 5*time.Minute)
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
	notifyTask := rangetask.NewNotifyDeleteRangeTask(c.store, startKey, endKey, concurrency)
	err = notifyTask.Execute(ctx)
	if err != nil {
		utils.ZapLog.Error("failed notifying regions affected by UnsafeDestroyRange",
			zap.Binary("start", startKey), zap.Binary("end", endKey), zap.Error(err))
		return UnsafeDestroyRangeFailed
	}
	return nil
}
