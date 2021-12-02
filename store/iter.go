package store

import (
	"bytes"
	"container/heap"

	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
)

type IterScan struct {
	prefix   []byte
	current  []byte
	curValue []byte
	idx      int
	iter     *Iterator
}

type IterList struct {
	iters map[int]*IterScan
	heap  *utils.BytesHeap
}

func NewIterList() *IterList {
	return &IterList{iters: make(map[int]*IterScan)}
}

func (i *IterList) Add(prefix []byte, idx int, iter *Iterator) {
	utils.ZapLog.Debug("IterList", zap.ByteString("prefix", prefix), zap.ByteString("start", iter.start), zap.ByteString("end", iter.end))
	i.iters[len(i.iters)] = &IterScan{prefix, prefix, nil, idx, iter}
}

func (i *IterList) Close() {
	if len(i.iters) == 0 {
		return
	}
	for _, iter := range i.iters {
		iter.iter.Close()
	}
}

func (i *IterList) Next() (utils.KV, error) {
	if i.heap == nil {
		i.heap = utils.NewBytesHeap()
		heap.Init(i.heap)
	}
	var err error
	var kv utils.KV
	for j, it := range i.iters {
		for it.iter.Valid() {
			cur := it.iter.Key()
			if len(cur) >= len(it.prefix) {
				ukv := utils.KV{
					Idx:   it.idx,
					Key:   cur[len(it.prefix):],
					Value: it.iter.Value(),
				}
				utils.ZapLog.Debug("IterList next", zap.ByteString("key", ukv.Key), zap.ByteString("value", ukv.Value))
				heap.Push(i.heap, ukv)
			}
			err = it.iter.Next()
			if err != nil {
				return kv, err
			}
			if len(cur) >= len(it.prefix) {
				break
			}
		}
		if !it.iter.Valid() {
			delete(i.iters, j)
		}
	}
	if i.heap.Len() == 0 {
		return kv, nil
	}
	return heap.Pop(i.heap).(utils.KV), nil
}

func (i *IterList) NextUntil(key []byte, all bool) (map[int][]byte, error) {
	utils.ZapLog.Debug("[txn] IterList NextUntil ", zap.ByteString("key", key), zap.Bool("all", all))
	var err error
	values := make(map[int][]byte)
	if len(i.iters) == 0 {
		return values, nil
	}
	for _, it := range i.iters {
		utils.ZapLog.Debug("[txn] IterList NextUntil ", zap.ByteString("key", key),
			zap.ByteString("prefix", it.prefix), zap.Int("len", len(i.iters)),
			zap.ByteString("current", it.current[len(it.prefix):]))
		if len(it.current) >= len(it.prefix) {
			c := bytes.Compare(it.current[len(it.prefix):], key)
			if c == 0 {
				values[it.idx] = it.curValue
				if !all {
					return values, nil
				}
				continue
			} else if c > 0 && all {
				return nil, nil
			}
			if c >= 0 {
				continue
			}
		}

		for it.iter.Valid() {
			it.current = it.iter.Key()
			it.curValue = it.iter.Value()
			utils.ZapLog.Debug("[txn] IterList NextUntil ", zap.String("remote", it.iter.txn.RemoteAddr()),
				zap.Uint64("timestamp", it.iter.txn.Timestamp), zap.ByteString("key", it.current),
				zap.ByteString("value", it.curValue))
			err = it.iter.Next()
			if err != nil {
				return nil, err
			}

			if len(it.current) >= len(it.prefix) {
				c := bytes.Compare(it.current[len(it.prefix):], key)
				if c < 0 {
					continue
				} else if c == 0 {
					values[it.idx] = it.curValue
					if !all {
						return values, nil
					}
					break
				} else if c > 0 && all {
					return nil, nil
				} else {
					break
				}
			}
		}
	}
	return values, nil
}

type LeftRight struct {
	Prefix []byte
	Left   *Iterator
	Right  *Iterator
}

func (i *LeftRight) Next() ([][]byte, [][]byte, error) {
	var err error
	var left, right [][]byte
	if i.Left != nil && i.Left.Valid() {
		left = make([][]byte, 2)
		left[0] = i.Left.Key()
		if len(left[0]) <= len(i.Prefix) {
			i.Left = nil
		} else {
			left[1] = i.Left.Value()
			utils.ZapLog.Debug("[txn] LeftRight left", zap.ByteString("key", left[0]), zap.ByteString("value", left[1]))
			err = i.Left.Next()
			if err != nil {
				return nil, nil, err
			}
		}
	}
	if i.Right != nil && i.Right.Valid() {
		if len(right[0]) <= len(i.Prefix) {
			i.Right = nil
		} else {
			right = make([][]byte, 2)
			right[0] = i.Right.Key()
			right[1] = i.Right.Value()
			utils.ZapLog.Debug("[txn] LeftRight right", zap.ByteString("key", right[0]), zap.ByteString("value", right[1]))
			err = i.Right.Next()
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return left, right, nil
}
