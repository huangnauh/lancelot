package store

import (
	"bytes"
	"net/url"
	"strconv"
	"strings"

	tikvConfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/lancelot/config"
)

type Client struct {
	store kv.Storage
	Conf  *config.Store
}

func Open(conf *config.Store) (*Client, error) {
	u, err := url.Parse(conf.Path)
	if err != nil {
		return nil, err
	}

	if strings.EqualFold(u.Scheme, "mocktikv") {
		var driver mockstore.MockTiKVDriver
		s, err := driver.Open(conf.Path)
		if err != nil {
			logrus.Errorf("mocktikv driver open %s", err)
			return nil, err
		}
		return &Client{s, conf}, nil
	}

	driver := tikv.Driver{}
	cfg := tikvConfig.GetGlobalConfig()
	cfg.Log.Level = conf.Level
	cfg.Log.EnableSlowLog = false
	tikvConfig.StoreGlobalConfig(cfg)
	err = logutil.InitZapLogger(cfg.Log.ToLogConfig())
	if err != nil {
		return nil, err
	}
	s, err := driver.Open(conf.Path)
	if err != nil {
		logrus.Errorf("tikv driver open %s", err)
		return nil, err
	}
	return &Client{s, conf}, nil
}

func (c *Client) NewTxn() *Txn {
	return &Txn{client: c}
}

func (c *Client) Close() {
	c.store.Close()
}

func (c *Client) CurrentVersion() (uint64, error) {
	ver, err := c.store.CurrentVersion(oracle.GlobalTxnScope)
	if err != nil {
		return 0, err
	}
	return ver.Ver, nil
}

func (c *Client) GetSafePointKV() tikv.SafePointKV {
	store, ok := c.store.(tikv.Storage)
	var kv tikv.SafePointKV
	if !ok {
		kv = tikv.NewMockSafePointKV()
	} else {
		kv = store.GetSafePointKV()
	}
	return kv
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
	logrus.Debugf("save ts %s %d", savedPath, t)
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

func (c *Client) DelteRange(start, end []byte, callback ClientCallback) {
	cur := start
	var count int
	var err error
	for bytes.Compare(cur, start) >= 0 && bytes.Compare(cur, end) < 0 {
		cur, count, err = c.DeleteUntil(cur, end, c.Conf.BatchLimit)
		if err != nil {
			return
		}
		if count < c.Conf.BatchLimit {
			break
		}
	}
	if callback != nil {
		callback(c)
	}
}

func (c *Client) DeleteUntil(start, end []byte, limit int) ([]byte, int, error) {
	txn := c.NewTxn()
	err := txn.Begin()
	if err != nil {
		logrus.Errorf("new txn err: %s", err)
		return start, 0, err
	}
	defer txn.Rollback()
	it, err := txn.Iter(start, end, false)
	if err != nil {
		logrus.Errorf("iter err: %s", err)
		return start, 0, err
	}
	defer it.Close()
	cur, count, err := it.DeleteUntil(limit)
	if err != nil {
		return cur, count, err
	}
	err = txn.Commit()
	if err != nil {
		logrus.Errorf("commit err: %s", err)
	}
	return cur, count, err
}

func (c *Client) Delete(key []byte) error {
	txn := c.NewTxn()
	err := txn.Begin()
	if err != nil {
		logrus.Errorf("new txn err: %s", err)
		return err
	}
	defer txn.Rollback()
	err = txn.Del(key)
	if err != nil {
		logrus.Errorf("del %s err: %s", key, err)
		return err
	}
	if err = txn.Commit(); err != nil {
		logrus.Errorf("commit err: %s", err)
		return err
	}
	return nil
}
