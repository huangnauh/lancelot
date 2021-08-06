package command

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

func (c *Command) GetCountBytes(typo ObjectType, data ...[]byte) []byte {
	count := 0
	for _, v := range data {
		count += len(v)
	}
	k := make([]byte, 1+1+count)
	k[0] = byte(CountPrefix)
	k[1] = byte(typo)
	start := 2
	for _, v := range data {
		copy(k[start:], v)
		start += len(v)
	}
	return k
}

type Count struct {
	Timestamp uint64
	Value     int64
	Key       []byte
}

func EncodeCount(c *Count) []byte {
	k := make([]byte, 8+binary.MaxVarintLen64)
	binary.BigEndian.PutUint64(k, c.Timestamp)
	n := binary.PutVarint(k[8:], c.Value)
	return k[:8+n]
}

func DecodeCount(c *Count, k []byte) error {
	c.Timestamp = binary.BigEndian.Uint64(k)
	count, err := binary.ReadVarint(bytes.NewReader(k[8:]))
	if err != nil {
		return err
	}
	c.Value = count
	return nil
}

func (c *Command) AddCount(txn *store.Txn, typo ObjectType, hash uint64, key []byte, hashValue []byte, delta int64) (*Count, error) {
	count, err := c.GetCount(txn, typo, hash, key, hashValue)
	if err != nil && err != store.KeyNotFound {
		return count, err
	}
	value := count.Value + delta
	utils.ZapLog.Debug("add count", zap.String("object", string(typo)), zap.ByteString("key", count.Key),
		zap.Int64("delta", delta), zap.Int64("oldcount", count.Value), zap.Int64("newcount", value))
	count.Timestamp = txn.Timestamp
	count.Value = value
	err = txn.Put(count.Key, EncodeCount(count))
	return count, err
}

func (c *Command) ListCount(txn *store.Txn, typo ObjectType, hash uint16, key []byte) ([]*Count, error) {
	start := c.GetCountBytes(typo, key)
	end := utils.PrefixNext(start)
	counts := make([]*Count, 0)
	err := txn.List(start, end, int(hash+1), func(k []byte, v []byte) bool {
		count := &Count{Key: k}
		err := DecodeCount(count, v)
		if err != nil {
			utils.ZapLog.Error("decode count", zap.String("object", string(typo)),
				zap.ByteString("key", k), zap.ByteString("value", v), zap.Error(err))
			return true
		}
		count.Key = k
		counts = append(counts, count)
		return true
	})
	return counts, err
}

func (c *Command) DeleteCount(txn *store.Txn, typo ObjectType, hash uint16, key []byte, expire time.Time) error {
	start := c.GetCountBytes(typo, key)
	end := utils.PrefixNext(start)
	it, err := txn.Iter(start, end, false)
	if err != nil {
		return err
	}
	defer it.Close()
	notExpire := false
	sum := 0
	_, _, err = it.DeleteUntil(int(hash+1), func(k []byte, v []byte) bool {
		sum++
		count := &Count{Key: k}
		err := DecodeCount(count, v)
		if err != nil {
			utils.ZapLog.Error("decode count", zap.String("object", string(typo)),
				zap.ByteString("key", k), zap.ByteString("value", v), zap.Error(err))
			return true
		}
		if expire.IsZero() {
			return true
		}

		timestamp := oracle.ExtractPhysical(count.Timestamp)
		exist := time.Unix(timestamp/1e3, (timestamp%1e3)*1e6)
		if expire.Sub(exist) > 0 {
			return true
		}
		utils.ZapLog.Info("count not expired", zap.String("object", string(typo)),
			zap.ByteString("key", k), zap.Time("exist", exist), zap.Time("expire", expire))
		notExpire = true
		return false
	})

	utils.ZapLog.Debug("delete count", zap.String("object", string(typo)),
		zap.ByteString("start", start), zap.ByteString("end", end),
		zap.Bool("notExpire", notExpire), zap.Int("sum", sum))

	if notExpire {
		return xerror.ErrNotExpire
	}
	if err == store.ReachLimit {
		return nil
	}
	return err
}

func (c *Command) GetCount(txn *store.Txn, typo ObjectType, hash uint64, key []byte, hashValue []byte) (*Count, error) {
	var k []byte
	if hash > 0 {
		shard := utils.GetShard(hashValue, hash)
		shardBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(shardBytes, shard)
		k = c.GetCountBytes(typo, key, shardBytes)
	} else {
		k = c.GetCountBytes(typo, key)
	}
	count := &Count{Timestamp: txn.Timestamp, Key: k}
	b, err := txn.Get(k)
	if err != nil {
		return count, err
	}
	err = DecodeCount(count, b)
	if err != nil {
		utils.ZapLog.Error("decode count", zap.String("object", string(typo)),
			zap.ByteString("key", k), zap.ByteString("value", b), zap.Error(err))
		return count, err
	}
	utils.ZapLog.Debug("get count", zap.String("object", string(typo)), zap.ByteString("key", k), zap.Int64("count", count.Value))
	return count, nil
}
