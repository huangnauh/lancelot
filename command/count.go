package command

import (
	"bytes"
	"encoding/binary"
	"time"

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
}

func EncodeCount(c Count) []byte {
	k := make([]byte, 8+binary.MaxVarintLen64)
	binary.BigEndian.PutUint64(k, c.Timestamp)
	n := binary.PutVarint(k[8:], c.Value)
	return k[:8+n]
}

func DecodeCount(k []byte) (Count, error) {
	c := Count{}
	c.Timestamp = binary.BigEndian.Uint64(k)
	count, err := binary.ReadVarint(bytes.NewReader(k[8:]))
	if err != nil {
		return c, err
	}
	c.Value = count
	return c, nil
}

func (c *Command) AddCount(txn *store.Txn, typo ObjectType, hash uint64, key []byte, hashValue []byte, delta int64) (Count, error) {
	utils.ZapLog.Debug("add count", zap.String("object", string(typo)), zap.ByteString("key", key), zap.Int64("delta", delta))
	var k []byte
	if hash > 0 {
		shard := utils.GetShard(hashValue, hash)
		shardBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(shardBytes, shard)
		k = c.GetCountBytes(typo, key, shardBytes)
	} else {
		k = c.GetCountBytes(typo, key)
	}

	count := Count{Timestamp: txn.Timestamp}
	var value int64
	b, err := txn.Get(k)
	if err == store.KeyNotFound {
	} else if err != nil {
		return count, err
	} else {
		value, err = binary.ReadVarint(bytes.NewReader(b))
		if err != nil {
			return count, err
		}
	}
	value += delta
	count = Count{
		Timestamp: txn.Timestamp,
		Value:     value,
	}
	err = txn.Put(k, EncodeCount(count))
	return count, err
}

func (c *Command) ListCount(txn *store.Txn, typo ObjectType, hash uint16, key []byte) ([]Count, error) {
	start := c.GetCountBytes(typo, key)
	end := utils.PrefixNext(start)
	counts := make([]Count, 0)
	txn.List(start, end, int(hash+1), func(k []byte, v []byte) bool {
		count, err := DecodeCount(v)
		if err != nil {
			return true
		}
		counts = append(counts, count)
		return true
	})
	return counts, nil
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
	_, _, err = it.DeleteUntil(int(hash+1), func(k []byte, v []byte) bool {
		count, err := DecodeCount(v)
		if err != nil {
			return true
		}
		if expire.IsZero() {
			return true
		}

		timestamp := int64(count.Timestamp)
		exist := time.Unix(timestamp/1e3, (timestamp%1e3)*1e6)
		if expire.Sub(exist) > 0 {
			return true
		}
		utils.ZapLog.Info("count not expired", zap.String("object", string(typo)),
			zap.ByteString("key", k), zap.Time("exist", exist), zap.Time("expire", expire))
		notExpire = true
		return false
	})

	if notExpire {
		return xerror.ErrNotExpire
	}
	if err == store.ReachLimit {
		return nil
	}
	return err
}

func (c *Command) GetCount(txn *store.Txn, typo ObjectType, hash uint64, key []byte, hashValue []byte) (Count, error) {
	var k []byte
	if hash > 0 {
		shard := utils.GetShard(hashValue, hash)
		shardBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(shardBytes, shard)
		k = c.GetCountBytes(typo, key, shardBytes)
	} else {
		k = c.GetCountBytes(typo, key)
	}
	count := Count{Timestamp: txn.Timestamp}
	b, err := txn.Get(k)
	if err == store.KeyNotFound {
		return count, nil
	} else if err != nil {
		return count, err
	}
	count, err = DecodeCount(b)
	if err != nil {
		return count, err
	}
	return count, nil
}
