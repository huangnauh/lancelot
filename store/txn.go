package store

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"github.com/tikv/client-go/v2/util"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const DefaultLockWait = 100

// type RespFunc func(txn *Txn)

type Txn struct {
	*redcon.Conn
	client     *Client
	txn        *transaction.KVTxn
	Multi      bool
	Watch      bool
	Exec       bool
	Err        error
	PendingErr bool
	InScript   bool
	Timestamp  uint64
	Now        int64
	ListLID    uint32
	ListRID    uint32
	Config     *config.Config
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

// func (t *Txn) GetListLID() uint32 {
// 	return atomic.AddUint32(&t.ListLID, 1)
// }

// func (t *Txn) GetListRID() uint32 {
// 	return atomic.AddUint32(&t.ListRID, 1)
// }

// func (t *Txn) GetCurrentUID() []byte {
// 	id := t.GetListaID()
// 	k := make([]byte, 8+4)
// 	binary.BigEndian.PutUint64(k[0:], uint64(t.Timestamp))
// 	binary.BigEndian.PutUint32(k[8:], id)
// 	return k
// }

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

func (t *Txn) SetConfig(cfg *config.Config) {
	t.Config = cfg
}

func (t *Txn) IsSkipConflict() bool {
	cfg := t.Config
	if cfg == nil {
		c := config.GetDefaultConfig()
		cfg = &c
	}
	return cfg.Redis.SkipConflict
}

func (t *Txn) IsPessimistic() bool {
	cfg := t.Config
	if cfg == nil {
		c := config.GetDefaultConfig()
		cfg = &c
	}
	if cfg.Redis.SkipConflict {
		return true
	}
	return cfg.Store.IsPessimistic
}

func ErrorEqual(err1, err2 error) bool {
	e1 := errors.Cause(err1)
	e2 := errors.Cause(err2)

	if e1 == e2 {
		return true
	}
	if e1 == nil || e2 == nil {
		return e1 == e2
	}
	return false
}

func returnErr(err error) error {
	if tikverr.IsErrWriteConflict(err) {
		return xerror.ErrKeyIsLocked
	}
	if ErrorEqual(err, tikverr.ErrLockAcquireFailAndNoWaitSet) ||
		ErrorEqual(err, tikverr.ErrLockWaitTimeout) {
		return xerror.ErrKeyIsLocked
	}
	if strings.HasPrefix(err.Error(), "2PC prewrite lockedKeys") {
		return xerror.ErrKeyIsLocked
	}
	return err
}

func (t *Txn) Begin() error {
	tx, err := t.client.store.Begin()
	if err != nil {
		utils.ZapLog.Error("[txn] client begin", zap.String("remote", t.RemoteAddr()), zap.Error(err))
		return err
	}
	if t.IsPessimistic() {
		tx.SetPessimistic(true)
	}
	tx.SetVars(t.client.disableLockVars)
	startTs := tx.StartTS()
	t.Timestamp = startTs
	t.Now = oracle.ExtractPhysical(startTs)
	t.txn = tx
	t.ListLID = 0
	t.ListRID = 0
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
		return returnErr(err)
	}
	utils.ZapLog.Debug("[txn] commit", zap.String("remote", t.RemoteAddr()),
		zap.Uint64("timestamp", t.Timestamp))
	t.txn = nil
	return nil
}

func (t *Txn) Get(key []byte) ([]byte, error) {
	if len(key) >= utils.MAX_KEY_SIZE {
		return nil, xerror.ErrExceedMaxSize
	}
	start := time.Now()
	snapshot := t.txn.GetSnapshot()
	snapshotStats := &txnsnapshot.SnapshotRuntimeStats{}
	snapshot.SetRuntimeStats(snapshotStats)
	ctx, cancel := context.WithTimeout(context.Background(), t.client.conf.ReadTimeout)
	execDetail := &util.ExecDetails{}
	defer cancel()
	v, err := t.txn.Get(ctx, key)
	utils.ZapLog.Debug("[txn] get", zap.String("remote", t.RemoteAddr()),
		zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.ByteString("value", v))
	spend := time.Since(start)
	if spend > t.client.conf.SlowRequest {
		utils.ZapLog.Warn("[txn] get slow request", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.Duration("spend", spend),
			zap.String("detail", execDetailsString(execDetail)), zap.Any("snapshot", snapshotStats))
	}

	if tikverr.IsErrNotFound(err) {
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
	if len(key) >= utils.MAX_KEY_SIZE {
		return xerror.ErrExceedMaxSize
	}
	if len(val) >= utils.MAX_VALUE_SIZE {
		return xerror.ErrExceedMaxSize
	}
	if t.IsPessimistic() {
		ctx, cancel := context.WithTimeout(context.Background(), t.client.conf.WriteTimeout)
		err := t.txn.LockKeysWithWaitTime(ctx, DefaultLockWait, key)
		cancel()
		if err != nil {
			utils.ZapLog.Error("[txn] lock", zap.String("remote", t.RemoteAddr()),
				zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.Error(err))
			return returnErr(err)
		}
	}
	err := t.txn.Set(key, val)
	if err != nil {
		utils.ZapLog.Error("[txn] set", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.Error(err))
		return returnErr(err)
	}
	utils.ZapLog.Debug("[txn] set", zap.String("remote", t.RemoteAddr()),
		zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.ByteString("value", val))
	return nil
}

func (t *Txn) Del(key []byte) error {
	if len(key) >= utils.MAX_KEY_SIZE {
		return xerror.ErrExceedMaxSize
	}
	if t.IsPessimistic() {
		ctx, cancel := context.WithTimeout(context.Background(), t.client.conf.WriteTimeout)
		err := t.txn.LockKeysWithWaitTime(ctx, DefaultLockWait, key)
		cancel()
		if err != nil {
			utils.ZapLog.Error("[txn] lock", zap.String("remote", t.RemoteAddr()),
				zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.Error(err))
			return returnErr(err)
		}
	}
	err := t.txn.Delete(key)
	if err != nil {
		utils.ZapLog.Error("[txn] del", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.Error(err))
		return returnErr(err)
	}
	utils.ZapLog.Debug("[txn] del", zap.String("remote", t.RemoteAddr()),
		zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key))
	return nil
}

func (t *Txn) LockKeys(keys [][]byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.client.conf.WriteTimeout)
	defer cancel()
	for i := range keys {
		if len(keys[i]) >= utils.MAX_KEY_SIZE {
			return xerror.ErrExceedMaxSize
		}
	}
	err := t.txn.LockKeys(ctx, new(kv.LockCtx), keys...)
	if err != nil {
		utils.ZapLog.Error("[txn] lock", zap.String("remote", t.RemoteAddr()),
			zap.Uint64("timestamp", t.Timestamp), zap.Error(err), zap.Any("keys", keys))
		return returnErr(err)
	}
	utils.ZapLog.Debug("[txn] lock", zap.String("remote", t.RemoteAddr()),
		zap.Uint64("timestamp", t.Timestamp), zap.Any("keys", keys))
	return nil
}

func (t *Txn) Reset() {
	if t.txn != nil {
		t.txn.Reset()
	}
}

type Iterator struct {
	tikv.Iterator
	start []byte
	end   []byte
	txn   *Txn
}

func (t *Txn) Iter(start, end []byte, reversed bool, scanSize int) (*Iterator, error) {
	var it tikv.Iterator
	if scanSize > 0 {
		snapshot := t.txn.GetSnapshot()
		snapshot.SetScanBatchSize(scanSize)
	}
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
	var it tikv.Iterator
	var err error
	if bytes.Compare(end, start) >= 0 {
		it, err = t.Iter(start, end, false, 0)
	} else {
		start, end = end, start
		it, err = t.Iter(start, end, true, 0)
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
		// utils.ZapLog.Debug("[txn] list ", zap.String("remote", t.RemoteAddr()),
		// 	zap.Uint64("timestamp", t.Timestamp), zap.ByteString("key", key), zap.ByteString("value", val))

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
