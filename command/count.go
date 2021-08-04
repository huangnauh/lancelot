package command

import (
	"bytes"
	"encoding/binary"

	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
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

func (c *Command) AddCount(txn *store.Txn, typo ObjectType, hash uint64, key []byte, delta int64) (int64, error) {
	var k []byte
	if hash > 0 {
		shard := utils.GetShard(key, hash)
		shardBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(shardBytes, shard)
		k = c.GetCountBytes(typo, key, shardBytes)
	} else {
		k = c.GetCountBytes(typo, key)
	}

	var count int64
	b, err := txn.Get(k)
	if err == store.KeyNotFound {
	} else if err != nil {
		return 0, err
	} else {
		count, err = binary.ReadVarint(bytes.NewReader(b))
		if err != nil {
			return 0, err
		}
	}
	count += delta
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, count)
	err = txn.Put(k, buf[:n])
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (c *Command) GetCount(txn *store.Txn, typo ObjectType, hash uint64, key []byte) (int64, error) {
	var k []byte
	if hash > 0 {
		shard := utils.GetShard(key, hash)
		shardBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(shardBytes, shard)
		k = c.GetCountBytes(typo, key, shardBytes)
	} else {
		k = c.GetCountBytes(typo, key)
	}
	b, err := txn.Get(k)
	if err == store.KeyNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	count, err := binary.ReadVarint(bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	return count, nil
}
