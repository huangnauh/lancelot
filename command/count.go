package command

import (
	"encoding/binary"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

func GetCountUserPrefix(user uint16) []byte {
	k := make([]byte, 1+2)
	k[0] = CountPrefix
	binary.BigEndian.PutUint16(k[1:], user)
	return k
}

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
		if data == nil {
			continue
		}
		copy(k[start:], v)
		start += len(v)
	}
	return k
}

func (c *Command) GetUserDBCountBytes(typo ObjectType, userID uint16, dbID uint8, data ...[]byte) []byte {
	count := 0
	for _, v := range data {
		count += len(v)
	}
	k := make([]byte, 1+2+1+1+count)
	k[0] = byte(CountPrefix)
	binary.BigEndian.PutUint16(k[1:], userID)
	k[3] = byte(dbID)
	k[4] = byte(typo)
	start := 5
	for _, v := range data {
		if data == nil {
			continue
		}
		copy(k[start:], v)
		start += len(v)
	}
	return k
}

type Count struct {
	Timestamp uint64
	Value     int64
	Key       []byte
	Shard     uint16
	UserValue []byte
}

func EncodeCount(c *Count) []byte {
	k := make([]byte, 8+1+8+len(c.UserValue))
	binary.BigEndian.PutUint64(k, c.Timestamp)
	var value uint64
	if c.Value >= 0 {
		k[8] = 0x01
		value = uint64(c.Value)
	} else {
		k[8] = 0x00
		value = uint64(-c.Value)
	}
	binary.BigEndian.PutUint64(k[9:], value)
	copy(k[17:], c.UserValue)
	return k
}

func DecodeCount(c *Count, v []byte) {
	c.Timestamp = binary.BigEndian.Uint64(v)
	if v[8] == 0x01 {
		c.Value = int64(binary.BigEndian.Uint64(v[9:]))
	} else {
		c.Value = -int64(binary.BigEndian.Uint64(v[9:]))
	}
	c.UserValue = v[17:]
}

func (c *Command) SetCount(txn *store.Txn, count *Count) error {
	utils.ZapLog.Debug("set count", zap.Uint64("timestamp", count.Timestamp), zap.Int64("count", count.Value),
		zap.ByteString("key", count.Key), zap.ByteString("user value", count.UserValue))
	count.Timestamp = txn.Timestamp
	err := txn.Put(count.Key, EncodeCount(count))
	return err
}

func (c *Command) ListCount(txn *store.Txn, userID uint16, dbID uint8, typo ObjectType, hash uint16, key []byte) ([]*Count, error) {
	binary.BigEndian.PutUint16(key, userID)
	start := c.GetUserDBCountBytes(typo, userID, dbID, key)
	end := utils.PrefixNext(start)
	counts := make([]*Count, 0)
	err := txn.List(start, end, int(hash+1), func(k []byte, v []byte) bool {
		count := &Count{Key: k}
		DecodeCount(count, v)
		count.Key = k
		counts = append(counts, count)
		return true
	})
	return counts, err
}

func (c *Command) DeleteCount(txn *store.Txn, userID uint16, dbID uint8, typo ObjectType, hash uint16, key []byte, expire time.Time) error {
	start := c.GetUserDBCountBytes(typo, userID, dbID, key)
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
		DecodeCount(count, v)
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

func (c *Command) GetCount(txn *store.Txn, userID uint16, dbID uint8, typo ObjectType, hash uint64, key []byte, hashValue []byte) (*Count, error) {
	var k []byte
	var shard uint16
	if hash > 0 {
		if hashValue != nil {
			shard = utils.GetShard(hashValue, hash)
		} else {
			shard = uint16(hash)
		}
		shardBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(shardBytes, shard)
		k = c.GetUserDBCountBytes(typo, userID, dbID, key, shardBytes)
	} else {
		k = c.GetUserDBCountBytes(typo, userID, dbID, key)
	}
	count := &Count{Timestamp: txn.Timestamp, Key: k, Shard: shard}
	b, err := txn.Get(k)
	if err != nil {
		return count, err
	}
	DecodeCount(count, b)
	utils.ZapLog.Debug("get count", zap.String("object", string(typo)), zap.ByteString("key", k), zap.Int64("count", count.Value))
	return count, nil
}
