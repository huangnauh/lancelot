package store

import (
	"context"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/redcon"
)

type RespFunc func(conn redcon.Conn)

type Txn struct {
	client      *Client
	txn         kv.Transaction
	Multi       bool
	Exec        bool
	Err         error
	PendingReq  []redcon.Command
	PendingResp []RespFunc
}

// type Transaction interface {
// 	Get(key []byte) ([]byte, error)
// 	Put(key, val []byte) error
// 	Del(key []byte) error
// 	LockKeys(keys [][]byte) error
// 	Commit() error
// 	HasTransaction() bool
// }

func (t *Txn) HasTransaction() bool {
	return t.txn != nil
}

func (t *Txn) Begin() error {
	logrus.Debugf("%p begin", t)
	tx, err := t.client.store.Begin()
	if err != nil {
		logrus.Errorf("client begin failed %s", err)
		return err
	}
	t.txn = tx
	return nil
}

func (t *Txn) Commit() error {
	logrus.Debugf("%p commit", t)
	ctx, cancel := context.WithTimeout(context.Background(), t.client.Conf.WriteTimeout)
	defer cancel()
	err := t.txn.Commit(ctx)
	if err != nil {
		logrus.Errorf("commit failed, err: %s", err)
		return err
	}
	return nil
}

func (t *Txn) Get(key []byte) ([]byte, error) {
	logrus.Debugf("%p get %s", t, key)
	start := time.Now()
	startTs := t.txn.StartTS()
	snapshot := t.txn.GetSnapshot()
	snapshotStats := &tikv.SnapshotRuntimeStats{}
	snapshot.SetOption(kv.CollectRuntimeStats, snapshotStats)
	ctx, cancel := context.WithTimeout(context.Background(), t.client.Conf.ReadTimeout)
	execDetail := &execdetails.StmtExecDetails{}
	ctx = context.WithValue(ctx, execdetails.StmtExecDetailKey, execDetail)
	defer cancel()
	v, err := t.txn.Get(ctx, key)

	spend := time.Since(start)
	if spend > t.client.Conf.SlowRequest {
		logrus.Warnf("get %s, start_ts %d, slow request %s %s, snapshot %s",
			key, startTs, spend, execDetailsString(execDetail), snapshotStats)
	}

	if kv.IsErrNotFound(err) {
		logrus.Debugf("%p get %s not found", t, key)
		return nil, KeyNotFound
	}
	if err != nil {
		logrus.Errorf("get %s failed %s", key, err)
		return nil, err
	}

	logrus.Debugf("%p get %s %s", t, key, v)
	return v, nil
}

func (t *Txn) Put(key, val []byte) error {
	logrus.Debugf("%p set %s %s", t, key, val)
	err := t.txn.Set(key, val)
	if err != nil {
		logrus.Errorf("set %s failed %s", key, err)
		return err
	}
	return nil
}

func (t *Txn) Del(key []byte) error {
	err := t.txn.Delete(key)
	if err != nil {
		logrus.Errorf("del %s failed %s", key, err)
		return err
	}
	return nil
}

func (t *Txn) LockKeys(keys [][]byte) error {
	kvKeys := make([]kv.Key, len(keys))
	for i := range keys {
		kvKeys[i] = kv.Key(keys[i])
	}
	return t.txn.LockKeys(context.Background(), new(kv.LockCtx), kvKeys...)
}
