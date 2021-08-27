package store

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/execdetails"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
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
	CurrentID  uint32
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

func (t *Txn) GetCurrentID() uint32 {
	return atomic.AddUint32(&t.CurrentID, 1)
}

func (t *Txn) GetCurrentUID() []byte {
	id := t.GetCurrentID()
	k := make([]byte, 8+4)
	binary.BigEndian.PutUint64(k[0:], uint64(t.Timestamp))
	binary.BigEndian.PutUint32(k[8:], id)
	return k
}

func (t *Txn) RemoteAddr() string {
	if t.Conn != nil {
		return t.Conn.RemoteAddr()
	}
	return "nil"
}

func (t *Txn) HasTransaction() bool {
	return t.txn != nil
}

func (t *Txn) NowTime() time.Time {
	return time.Unix(t.Now/1e3, (t.Now%1e3)*1e6)
}

func (t *Txn) Begin() error {
	tx, err := t.client.store.Begin()
	if err != nil {
		utils.ZapLog.Error("[txn] client begin", zap.String("remote", t.RemoteAddr()), zap.Error(err))
		return err
	}
	startTs := tx.StartTS()
	t.Timestamp = startTs
	t.Now = oracle.ExtractPhysical(startTs)
	t.txn = tx
	return nil
}

func (t *Txn) Rollback() {
	if t.txn != nil {
		_ = t.txn.Rollback()
		t.txn = nil
	}
}

func (t *Txn) Commit() error {
	if t.txn == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.client.conf.WriteTimeout)
	defer cancel()
	err := t.txn.Commit(ctx)
	if err != nil {
		utils.ZapLog.Error("[txn] commit", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.Error(err))
		t.Rollback()
		return err
	}
	utils.ZapLog.Debug("[txn] commit", zap.String("remote", t.RemoteAddr()),
		zap.Uint64("timestamp", t.Timestamp))
	t.txn = nil
	return nil
}

func (t *Txn) Get(key []byte) ([]byte, error) {
	start := time.Now()
	snapshot := t.txn.GetSnapshot()
	snapshotStats := &tikv.SnapshotRuntimeStats{}
	snapshot.SetOption(kv.CollectRuntimeStats, snapshotStats)
	ctx, cancel := context.WithTimeout(context.Background(), t.client.conf.ReadTimeout)
	execDetail := &execdetails.StmtExecDetails{}
	ctx = context.WithValue(ctx, execdetails.StmtExecDetailKey, execDetail)
	defer cancel()
	v, err := t.txn.Get(ctx, key)

	spend := time.Since(start)
	if spend > t.client.conf.SlowRequest {
		utils.ZapLog.Warn("[txn] get slow request", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.Duration("spend", spend),
			zap.String("detail", execDetailsString(execDetail)), zap.Any("snapshot", snapshotStats))
	}

	if kv.IsErrNotFound(err) {
		return nil, KeyNotFound
	}
	if err != nil {
		utils.ZapLog.Error("[txn] get", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.Error(err))
		return nil, err
	}
	return v, nil
}

func (t *Txn) Put(key, val []byte) error {
	err := t.txn.Set(key, val)
	if err != nil {
		utils.ZapLog.Error("[txn] set", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.Error(err))
		return err
	}
	utils.ZapLog.Debug("[txn] set", zap.String("remote", t.RemoteAddr()),
		zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.ByteString("value", val))
	return nil
}

func (t *Txn) Del(key []byte) error {
	err := t.txn.Delete(key)
	if err != nil {
		utils.ZapLog.Error("[txn] del", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.Error(err))
		return err
	}
	utils.ZapLog.Debug("[txn] del", zap.String("remote", t.RemoteAddr()),
		zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key))
	return nil
}

func (t *Txn) LockKeys(keys [][]byte) error {
	kvKeys := make([]kv.Key, len(keys))
	for i := range keys {
		kvKeys[i] = kv.Key(keys[i])
	}
	err := t.txn.LockKeys(context.Background(), new(kv.LockCtx), kvKeys...)
	if err != nil {
		utils.ZapLog.Error("[txn] lock", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.Error(err))
	}
	return err
}

type Iterator struct {
	kv.Iterator
	start []byte
	end   []byte
	txn   *Txn
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
	return &Iterator{it, start, end, t}, nil
}

func (t *Txn) List(start, end []byte, limit int, callback KVCallback) error {
	var it kv.Iterator
	var err error
	if bytes.Compare(end, start) >= 0 {
		it, err = t.Iter(start, end, false)
	} else {
		start, end = end, start
		it, err = t.Iter(start, end, true)
	}
	if err != nil {
		utils.ZapLog.Error("[txn] iter", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.ByteString("start", start), zap.ByteString("end", end),
			zap.Error(err))
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
		utils.ZapLog.Debug("[txn] list ", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.ByteString("value", val))

		if callback != nil {
			ok := callback(key, val)
			if !ok {
				return nil
			}
		}

		count++
		if limit > 0 && count >= limit {
			return ReachLimit
		}
		err = it.Next()
		if err != nil {
			utils.ZapLog.Error("[txn] iter next", zap.String("remote", t.RemoteAddr()),
				zap.Uint64("timestamp", t.Timestamp), zap.ByteString("start", start), zap.ByteString("end", end),
				zap.Error(err))
			return err
		}
	}
	return nil
}

func (t *Iterator) DeleteUntil(limit int, callback KVCallback) (key []byte, count int, err error) {
	for t.Valid() {
		key = t.Key()
		if bytes.Compare(key, t.start) < 0 || bytes.Compare(key, t.end) >= 0 {
			return
		}
		val := t.Value()
		utils.ZapLog.Debug("[txn] list ", zap.String("remote", t.txn.RemoteAddr()),
			zap.Uint64("timestamp", t.txn.Timestamp), zap.ByteString("key", key), zap.ByteString("value", val))

		if callback != nil {
			ok := callback(key, val)
			if !ok {
				return
			}
		}

		err = t.txn.Del(key)
		if err != nil {
			utils.ZapLog.Error("[txn] del", zap.String("remote", t.txn.RemoteAddr()),
				zap.Uint64("timestamp", t.txn.Timestamp), zap.ByteString("start", t.start), zap.ByteString("end", t.end),
				zap.Error(err))
			return
		}
		count++
		if limit > 0 && count >= limit {
			return
		}
		err = t.Next()
		if err != nil {
			utils.ZapLog.Error("[txn] iter next", zap.String("remote", t.txn.RemoteAddr()),
				zap.Uint64("timestamp", t.txn.Timestamp), zap.ByteString("start", t.start), zap.ByteString("end", t.end),
				zap.Error(err))
			return
		}
	}
	return
}

type IterScan struct {
	prefix  []byte
	current []byte
	iter    *Iterator
}
type IterList struct {
	iters map[int]*IterScan
	heap  *utils.BytesHeap
}

func NewIterList() *IterList {
	return &IterList{}
}

func (i *IterList) Add(prefix []byte, iter *Iterator) {
	i.iters[len(i.iters)] = &IterScan{prefix, prefix, iter}
}

func (i *IterList) Close() {
	if len(i.iters) == 0 {
		return
	}
	for _, iter := range i.iters {
		iter.iter.Close()
	}
}

func (i *IterList) Next() ([]byte, error) {
	if i.heap == nil {
		i.heap = utils.NewBytesHeap()
		heap.Init(i.heap)
	}
	for j, it := range i.iters {
		for it.iter.Valid() {
			cur := it.iter.Key()
			if len(cur) < len(it.prefix) {
				continue
			}
			heap.Push(i.heap, cur[len(it.prefix):])
			break
		}
		if !it.iter.Valid() {
			delete(i.iters, j)
		}
	}
	if i.heap.Len() == 0 {
		return nil, nil
	}
	return i.heap.Pop().([]byte), nil
}

func (i *IterList) NextUntil(key []byte, all bool) (bool, error) {
	var err error
	for j, it := range i.iters {
		c := bytes.Compare(it.current, key)
		if c == 0 && !all {
			return true, nil
		} else if c > 0 && all {
			return false, nil
		}
		if c >= 0 {
			continue
		}

		for it.iter.Valid() {
			it.current = it.iter.Key()
			if len(key) >= len(it.prefix) {
				c := bytes.Compare(it.current, key[len(it.prefix):])
				if c < 0 {
					continue
				}

				if c == 0 && !all {
					return true, nil
				} else if c > 0 && all {
					return false, nil
				}
				break
			}
			err = it.iter.Next()
			if err != nil {
				return false, err
			}
		}

		if !it.iter.Valid() {
			delete(i.iters, j)
		}
	}
	return all, nil
}
