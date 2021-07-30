package store

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
)

// type RespFunc func(txn *Txn)

type Txn struct {
	*redcon.Conn
	client     *Client
	txn        kv.Transaction
	Multi      bool
	Exec       bool
	Err        error
	Timestamp  uint64
	Now        int64
	PendingReq []redcon.Command
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

func (t *Txn) NowTime() time.Time {
	return time.Unix(t.Now/1e3, (t.Now%1e3)*1e6)
}

func (t *Txn) Begin() error {
	logrus.Debugf("%p begin", t)
	tx, err := t.client.store.Begin()
	if err != nil {
		logrus.Errorf("client begin failed %s", err)
		return err
	}
	startTs := tx.StartTS()
	t.Timestamp = startTs
	t.Now = oracle.ExtractPhysical(startTs)
	t.txn = tx
	return nil
}

func (t *Txn) Rollback() {
	logrus.Debugf("%p rollback", t)
	if t.txn != nil {
		_ = t.txn.Rollback()
		t.txn = nil
	}
}

func (t *Txn) Commit() error {
	logrus.Debugf("%p commit", t)
	ctx, cancel := context.WithTimeout(context.Background(), t.client.Conf.WriteTimeout)
	defer cancel()
	err := t.txn.Commit(ctx)
	if err != nil {
		logrus.Errorf("commit failed, err: %s", err)
		t.Rollback()
		return err
	}
	t.txn = nil
	return nil
}

func (t *Txn) Get(key []byte) ([]byte, error) {
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
		logrus.Warnf("get %v, start_ts %d, slow request %s %s, snapshot %s",
			key, startTs, spend, execDetailsString(execDetail), snapshotStats)
	}

	if kv.IsErrNotFound(err) {
		logrus.Debugf("%p get %v not found", t, key)
		return nil, KeyNotFound
	}
	if err != nil {
		logrus.Errorf("get %v failed %s", key, err)
		return nil, err
	}

	logrus.Debugf("%p get %v %s", t, key, v)
	return v, nil
}

func (t *Txn) Put(key, val []byte) error {
	logrus.Debugf("%p set %v %s", t, key, val)
	err := t.txn.Set(key, val)
	if err != nil {
		logrus.Errorf("set %s failed %s", key, err)
		return err
	}
	return nil
}

func (t *Txn) Del(key []byte) error {
	logrus.Debugf("%p del %v", t, key)
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

type Iterator struct {
	start []byte
	end   []byte
	kv.Iterator
	txn *Txn
}

func (t *Txn) Iter(start, end []byte, reversed bool) (*Iterator, error) {
	var it kv.Iterator
	var err error
	if !reversed {
		it, err = t.txn.Iter(start, end)
	} else {
		it, err = t.txn.IterReverse(end)
	}
	if err != nil {
		return nil, err
	}
	return &Iterator{start, end, it, t}, nil
}

func (t *Txn) List(start, end []byte, limit int, callback KVCallback) error {
	it, err := t.Iter(start, end, false)
	if err != nil {
		logrus.Errorf("iter err: %s", err)
		return err
	}
	defer it.Close()

	count := 0
	for it.Valid() {
		key := it.Key()
		if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) >= 0 {
			return nil
		}
		val := it.Value()
		logrus.Debugf("%p list %s %s", t, []byte(key), val)
		ok := callback(key, val)
		if !ok {
			return nil
		}

		count++
		if limit > 0 && count >= limit {
			return ReachLimit
		}
		err = it.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Iterator) DeleteUntil(limit int) (key []byte, count int, err error) {
	for t.Valid() {
		key = t.Key()
		if bytes.Compare(key, t.start) < 0 || bytes.Compare(key, t.end) >= 0 {
			return
		}
		err = t.txn.Del(key)
		if err != nil {
			return
		}
		count++
		if limit > 0 && count >= limit {
			return
		}
		err = t.Next()
		if err != nil {
			return
		}
	}
	return
}
