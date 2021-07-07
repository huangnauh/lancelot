package store

import (
	"net/url"
	"strings"

	tikvConfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
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
