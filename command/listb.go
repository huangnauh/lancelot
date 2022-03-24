package command

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const (
	MaxScan      = 10000
	PullInternal = 100 * time.Millisecond
)

var (
	BListFuncs = map[string]ListFunc{
		LINDEX_COMMAND:  blIndex,
		LINSERT_COMMAND: blInsert,
		LPOP_COMMAND:    blPop,
		LPUSH_COMMAND:   blPush,
		RPOP_COMMAND:    brPop,
		RPUSH_COMMAND:   brPush,
		LRANGE_COMMAND:  blRange,
		LLEN_COMMAND:    blLen,
		LTRIM_COMMAND:   blTrim,
		LINFO_COMMAND:   blInfo,
		LREM_COMMAND:    blRem,
		LSET_COMMAND:    blSet,
		LPOS_COMMAND:    blPos,
	}
)

type ListObject struct {
	LIndex  float64
	RIndex  float64
	Length  uint64
	Complex bool
}

func (l *ListObject) Clear() {
	l.LIndex = 0
	l.RIndex = 0
	l.Length = 0
	l.Complex = false
}

type Value struct {
	Timestamp uint64
	Value     []byte
}

func EncodeValue(v *Value) []byte {
	k := make([]byte, 8+len(v.Value))
	binary.BigEndian.PutUint64(k[0:8], v.Timestamp)
	if len(v.Value) > 0 {
		copy(k[8:], v.Value)
	}
	return k
}

func DecodeValue(b []byte, v *Value) error {
	if len(b) < 8 {
		return xerror.ErrValueTooShort
	}
	v.Timestamp = binary.BigEndian.Uint64(b[0:8])
	v.Value = b[8:]
	return nil
}

type lOpt struct {
	index    []int64
	max      int
	count    int
	before   bool
	after    bool
	exist    bool
	readonly bool
	l        *ListObject
}

func EncodeListObjectExtra(l *ListObject) []byte {
	k := make([]byte, 8+8+8+1)
	binary.BigEndian.PutUint64(k[0:8], math.Float64bits(l.LIndex))
	binary.BigEndian.PutUint64(k[8:16], math.Float64bits(l.RIndex))
	binary.BigEndian.PutUint64(k[16:], l.Length)
	if l.Complex {
		k[24] = 1
	} else {
		k[24] = 0
	}
	return k
}

func DecodeListObjectExtra(k []byte) (*ListObject, error) {
	if len(k) != 25 {
		return nil, xerror.ErrValueTooShort
	}
	l := &ListObject{}
	l.LIndex = math.Float64frombits(binary.BigEndian.Uint64(k[0:8]))
	l.RIndex = math.Float64frombits(binary.BigEndian.Uint64(k[8:16]))
	l.Length = binary.BigEndian.Uint64(k[16:])
	if k[24] == 1 {
		l.Complex = true
	} else {
		l.Complex = false
	}
	return l, nil
}

type ListFunc func(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error)

func getStartEnd(l *ListObject, opt *lOpt) (int64, int64, error) {
	startIndex := opt.index[0]
	endIndex := opt.index[1]
	if startIndex < 0 {
		startIndex += int64(l.Length)
	}
	if startIndex < 0 {
		startIndex = 0
	}
	if startIndex >= int64(l.Length) {
		return 0, 0, xerror.ErrOutOfRange
	}
	if endIndex < 0 {
		endIndex += int64(l.Length)
	}
	if endIndex < 0 {
		return 0, 0, xerror.ErrStartGreaterThanEnd
	}
	if endIndex >= int64(l.Length) {
		endIndex = int64(l.Length) - 1
	}
	if startIndex > endIndex {
		return 0, 0, xerror.ErrStartGreaterThanEnd
	}
	return startIndex, endIndex, nil
}

func blTrim(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	l := opt.l
	startIndex, endIndex, err := getStartEnd(l, opt)
	if err == xerror.ErrStartGreaterThanEnd {
		startIndex = int64(l.Length)
		endIndex = int64(l.Length)
	} else if err != nil {
		utils.ZapLog.Error("getStartEnd", zap.Float64("left", l.LIndex), zap.Float64("right", l.RIndex), zap.Uint64("length", l.Length),
			zap.Int64("start", opt.index[0]), zap.Int64("end", opt.index[1]), zap.Error(err))
		return OK, nil
	}
	prefix := object.GetValueBytes(nil)
	var delErr error
	var count int64
	start := object.GetValueBytes(utils.EncodeFloat(l.LIndex))
	end := object.GetValueBytes(utils.EncodeFloat(l.RIndex + 1))
	if startIndex > 0 {
		err = txn.List(start, end, opt.max, func(key, value []byte) bool {
			if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
				return false
			}
			if count >= startIndex {
				l.LIndex = utils.DecodeFloat(key[len(prefix):])
				return false
			}
			utils.ZapLog.Debug("ltrim-start", zap.Int64("start", startIndex), zap.Int64("end", endIndex), zap.Int64("count", count),
				zap.ByteString("key", key))
			delErr = txn.Del(key)
			if delErr != nil {
				return false
			}
			count++
			return true
		})
		if err != nil {
			return nil, err
		}
		if delErr != nil {
			return nil, delErr
		}
	}

	if startIndex == int64(l.Length) {
		l.Length = 0
		return OK, nil
	}

	if endIndex >= int64(l.Length)-1 {
		l.Length = l.Length - uint64(startIndex)
		return OK, nil
	}

	if !l.Complex {
		start = object.GetValueBytes(utils.EncodeFloat(l.LIndex + float64(endIndex) + 1))
		err = txn.List(start, end, opt.max, func(key, value []byte) bool {
			if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
				return false
			}
			utils.ZapLog.Debug("ltrim-end", zap.Int64("start", startIndex), zap.Int64("end", endIndex), zap.Int64("count", count),
				zap.ByteString("key", key))
			delErr = txn.Del(key)
			return delErr == nil
		})
		l.RIndex -= float64(l.Length) - float64(endIndex) - 1
	} else {
		var count int64
		start, end = end, start
		err = txn.List(start, end, opt.max, func(key, value []byte) bool {
			if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
				return false
			}
			if count >= int64(l.Length)-endIndex-1 {
				l.RIndex = utils.DecodeFloat(key[len(prefix):])
				return false
			}
			utils.ZapLog.Debug("ltrim-end", zap.Int64("start", startIndex), zap.Int64("end", endIndex), zap.Int64("count", count),
				zap.ByteString("key", key))
			delErr = txn.Del(key)
			if delErr != nil {
				return false
			}
			count++
			return true
		})
	}

	if err != nil {
		return nil, err
	}
	if delErr != nil {
		return nil, delErr
	}

	l.Length = uint64(endIndex - startIndex + 1)
	return OK, nil
}

func blInfo(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	l := opt.l
	if len(args) == 0 {
		return []interface{}{
			redcon.SimpleInt(l.LIndex),
			redcon.SimpleInt(l.RIndex),
			redcon.SimpleInt(l.Length),
			l.Complex,
		}, nil
	}
	s, err := strconv.ParseFloat(string(args[0]), 64)
	if err != nil {
		return nil, nil
	}
	e, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return nil, nil
	}
	prefix := object.GetValueBytes(nil)
	start := object.GetValueBytes(utils.EncodeFloat(s))
	end := object.GetValueBytes(utils.EncodeFloat(e))
	ret := make([][]interface{}, 0)
	err = txn.List(start, end, opt.max, func(key, value []byte) bool {
		if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
			return true
		}
		lvalue := &Value{}
		DecodeValue(value, lvalue)
		ret = append(ret, []interface{}{
			utils.DecodeFloat(key[len(prefix):]),
			redcon.SimpleInt(lvalue.Timestamp), lvalue.Value})
		return true
	})
	return ret, err
}

func blRange(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	l := opt.l
	ret := make([][]byte, 0)
	startIndex, endIndex, err := getStartEnd(l, opt)
	if err != nil {
		return ret, nil
	}

	var s, e float64
	if !l.Complex {
		s = l.LIndex + float64(startIndex)
		e = l.LIndex + float64(endIndex+1)
	} else {
		s = l.LIndex
		e = l.RIndex + 1
	}

	prefix := object.GetValueBytes(nil)
	start := object.GetValueBytes(utils.EncodeFloat(s))
	end := object.GetValueBytes(utils.EncodeFloat(e))
	var count int64
	err = txn.List(start, end, opt.max, func(key, value []byte) bool {
		if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
			return true
		}
		lvalue := &Value{}
		DecodeValue(value, lvalue)
		utils.ZapLog.Debug("blRange", zap.Float64("start", s), zap.Float64("end", e), zap.Int("max", opt.max),
			zap.ByteString("key", key), zap.ByteString("value", lvalue.Value))
		if !l.Complex {
			ret = append(ret, lvalue.Value)
			return true
		}
		if count >= startIndex {
			ret = append(ret, lvalue.Value)
		}
		count++
		return count <= endIndex
	})
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func index(txn *store.Txn, object *Object, l *ListObject, opt *lOpt) (float64, error) {
	i := opt.index[0]
	if i >= int64(l.Length) {
		return 0, xerror.ErrOutOfRange
	}

	if i < 0 {
		i += int64(l.Length)
	}

	if i < 0 {
		return 0, xerror.ErrOutOfRange
	}

	if !l.Complex {
		return l.LIndex + float64(i), nil
	}
	index := uint64(i)
	if index == 0 {
		return l.LIndex, nil
	} else if index == l.Length-1 {
		return l.RIndex, nil
	}
	if index >= uint64(opt.max) {
		return 0, xerror.ErrOutOfRange
	}

	idx := math.MaxFloat64
	prefix := object.GetValueBytes(nil)
	start := object.GetValueBytes(utils.EncodeFloat(l.LIndex))
	end := object.GetValueBytes(utils.EncodeFloat(l.RIndex + 1))
	var count uint64
	err := txn.List(start, end, opt.max, func(key, value []byte) bool {
		if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
			return true
		}
		if count == index {
			idx = utils.DecodeFloat(key[len(prefix):])
			return false
		}
		count++
		return true
	})
	if err != nil {
		return 0, err
	}
	return idx, nil
}

func posValue(txn *store.Txn, object *Object, l *ListObject, v []byte,
	opt *lOpt) ([]redcon.SimpleInt, error) {
	index := opt.index[0]
	if index >= int64(l.Length) || index <= -int64(l.Length) {
		return nil, nil
	}
	if opt.count > int(l.Length) {
		opt.count = int(l.Length)
	}

	idxs := make([]redcon.SimpleInt, 0)
	start := object.GetValueBytes(utils.EncodeFloat(l.LIndex))
	end := object.GetValueBytes(utils.EncodeFloat(l.RIndex + 1))
	uindex := index
	if index >= 0 {

	} else {
		uindex = -index
		start, end = end, start
	}
	var count, idx int64
	begin := false
	err := txn.List(start, end, opt.max, func(key, value []byte) bool {
		idx++
		lvalue := &Value{}
		DecodeValue(value, lvalue)
		if !bytes.Equal(v, lvalue.Value) {
			return true
		}
		count++
		if count >= uindex {
			begin = true
		}
		if !begin {
			return true
		}
		if index >= 0 {
			idxs = append(idxs, redcon.SimpleInt(idx-1))
		} else {
			idxs = append(idxs, redcon.SimpleInt(int64(l.Length)-idx))
		}
		return len(idxs) < opt.count
	})
	if err != nil && err != store.ReachLimit {
		return nil, err
	}
	return idxs, nil
}

func indexValue(txn *store.Txn, object *Object, l *ListObject, v []byte,
	opt *lOpt) ([]float64, error) {
	prefix := object.GetValueBytes(nil)
	start := object.GetValueBytes(utils.EncodeFloat(l.LIndex))
	end := object.GetValueBytes(utils.EncodeFloat(l.RIndex + 1))
	idxs := []float64{math.MaxFloat64, math.MaxFloat64, math.MaxFloat64}
	found := false
	err := txn.List(start, end, opt.max, func(key, value []byte) bool {
		if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
			return true
		}
		if found {
			// next
			idxs[2] = utils.DecodeFloat(key[len(prefix):])
			return false
		}
		idxs[0] = idxs[1]
		idxs[1] = utils.DecodeFloat(key[len(prefix):])
		lvalue := &Value{}
		DecodeValue(value, lvalue)
		if bytes.Equal(v, lvalue.Value) {
			found = true
			needNext := opt.after
			if idxs[1] == l.RIndex {
				needNext = false
			}
			return needNext
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, xerror.ErrNotFound
	}
	return idxs, nil
}

func blPos(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	l := opt.l
	ids, err := posValue(txn, object, l, args[0], opt)
	if err != nil {
		return nil, err
	}
	if opt.count == 0 {
		if len(ids) == 0 {
			return nil, nil
		}
		return ids[0], nil
	}
	return ids, nil
}

func blIndex(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	l := opt.l
	index, err := index(txn, object, l, opt)
	if err == xerror.ErrOutOfRange {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	lkey := object.GetValueBytes(utils.EncodeFloat(index))
	v, err := txn.Get(lkey)
	if err != nil {
		return nil, err
	}
	lvalue := &Value{}
	DecodeValue(v, lvalue)
	return lvalue.Value, nil
}

func blLen(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	l := opt.l
	return redcon.SimpleInt(l.Length), nil
}

func blRem(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	l := opt.l
	prefix := object.GetValueBytes(nil)
	start := object.GetValueBytes(utils.EncodeFloat(l.LIndex))
	end := object.GetValueBytes(utils.EncodeFloat(l.RIndex + 1))
	var count int
	if opt.count >= 0 {
		if opt.count == 0 {
			count = int(l.Length)
		} else {
			count = opt.count
		}
	} else {
		count = -opt.count
		start, end = end, start
	}
	var c int
	var delErr error
	first := false
	var pre, cur float64
	err := txn.List(start, end, opt.max, func(key, value []byte) bool {
		if len(key) < len(prefix) {
			return true
		}
		cur = utils.DecodeFloat(key[len(prefix):])
		if c >= count {
			if opt.count >= 0 {
				l.LIndex = cur
			} else {
				l.RIndex = cur
			}
			return false
		}

		lvalue := &Value{}
		DecodeValue(value, lvalue)
		if !bytes.Equal(args[1], lvalue.Value) {
			pre = cur
			if !first {
				if opt.count >= 0 {
					l.LIndex = cur
				} else {
					l.RIndex = cur
				}
				first = true
			}
			return true
		}
		delErr = txn.Del(key)
		if delErr != nil {
			return false
		}
		c++
		if opt.count >= 0 && cur == l.RIndex {
			l.RIndex = pre
			return false
		}
		if opt.count < 0 && cur == l.LIndex {
			l.LIndex = pre
			return false
		}
		if c >= count {
			return !first
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if delErr != nil {
		return nil, delErr
	}
	if c == 0 {
		opt.readonly = true
		return 0, nil
	}
	l.Length -= uint64(c)
	l.Complex = true
	return redcon.SimpleInt(c), nil
}

func blInsert(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	l := opt.l
	ids, err := indexValue(txn, object, l, args[1], opt)
	if err == xerror.ErrNotFound {
		return redcon.SimpleInt(-1), nil
	}

	if err != nil {
		return nil, err
	}
	var index float64
	if opt.before {
		if ids[1] == l.LIndex {
			return blPush(txn, object, args[2:], opt)
		}
		index, err = utils.MiddleFloat(ids[0], ids[1])
	} else {
		if ids[1] == l.RIndex {
			return brPush(txn, object, args[2:], opt)
		}
		index, err = utils.MiddleFloat(ids[1], ids[2])
	}

	if err != nil {
		return nil, err
	}
	lkey := object.GetValueBytes(utils.EncodeFloat(index))
	lvalue := &Value{
		Value:     args[2],
		Timestamp: txn.Timestamp,
	}
	err = txn.Put(lkey, EncodeValue(lvalue))
	if err != nil {
		return nil, err
	}
	l.Complex = true
	l.Length++
	return redcon.SimpleInt(l.Length), nil
}

func blSet(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	l := opt.l
	index, err := index(txn, object, l, opt)
	if err != nil {
		return nil, err
	}
	lkey := object.GetValueBytes(utils.EncodeFloat(index))
	lvalue := &Value{
		Value:     args[1],
		Timestamp: txn.Timestamp,
	}
	err = txn.Put(lkey, EncodeValue(lvalue))
	if err != nil {
		return nil, err
	}
	return OK, nil
}

func blPush(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	var err error
	l := opt.l
	for _, msg := range args {
		if l.Length > 0 {
			l.LIndex--
		}
		lkey := object.GetValueBytes(utils.EncodeFloat(l.LIndex))
		lvalue := &Value{
			Value:     msg,
			Timestamp: txn.Timestamp,
		}
		err = txn.Put(lkey, EncodeValue(lvalue))
		if err != nil {
			return redcon.SimpleInt(0), err
		}
		l.Length++
	}
	return redcon.SimpleInt(l.Length), err
}

func brPush(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	var err error
	l := opt.l
	for _, msg := range args {
		if l.Length > 0 {
			l.RIndex++
		}
		lkey := object.GetValueBytes(utils.EncodeFloat(l.RIndex))
		lvalue := &Value{
			Value:     msg,
			Timestamp: txn.Timestamp,
		}
		err = txn.Put(lkey, EncodeValue(lvalue))
		if err != nil {
			return redcon.SimpleInt(0), err
		}
		l.Length++
	}
	return redcon.SimpleInt(l.Length), err
}

func blPop(txn *store.Txn, object *Object, _ [][]byte, opt *lOpt) (interface{}, error) {
	l := opt.l
	if l.Length == 0 {
		opt.readonly = true
		return nil, nil
	}
	count := opt.count
	if count == 0 {
		count = 1
	}
	if count > int(l.Length) {
		count = int(l.Length)
	}

	prefix := object.GetValueBytes(nil)
	start := object.GetValueBytes(utils.EncodeFloat(l.LIndex))
	end := object.GetValueBytes(utils.EncodeFloat(l.RIndex + 1))
	var delErr error
	ret := make([][]byte, 0)
	err := txn.List(start, end, count+1, func(key, value []byte) bool {
		if len(key) < len(prefix) {
			return true
		}
		if len(ret) >= count {
			l.LIndex = utils.DecodeFloat(key[len(prefix):])
			return false
		}
		delErr = txn.Del(key)
		if delErr != nil {
			return false
		}
		lvalue := &Value{}
		DecodeValue(value, lvalue)
		ret = append(ret, lvalue.Value)
		if len(ret) >= count {
			if !l.Complex {
				l.LIndex += float64(count)
				return false
			}
		}
		return true
	})
	if delErr != nil {
		return nil, delErr
	}
	if err != nil {
		return nil, err
	}
	l.Length -= uint64(len(ret))
	if opt.count == 0 {
		if len(ret) == 1 {
			return ret[0], nil
		}
		return nil, nil
	}
	return ret, nil
}

func brPop(txn *store.Txn, object *Object, _ [][]byte, opt *lOpt) (interface{}, error) {
	l := opt.l
	if l.Length == 0 {
		opt.readonly = true
		return nil, nil
	}
	count := opt.count
	if count == 0 {
		count = 1
	}
	if count > int(l.Length) {
		count = int(l.Length)
	}
	start := object.GetValueBytes(utils.EncodeFloat(l.RIndex + 1))
	prefix := object.GetValueBytes(nil)
	end := object.GetValueBytes(utils.EncodeFloat(l.LIndex))
	var delErr error
	ret := make([][]byte, 0)
	err := txn.List(start, end, count+1, func(key, value []byte) bool {
		if len(key) < len(prefix) {
			return true
		}
		if len(ret) >= count {
			l.RIndex = utils.DecodeFloat(key[len(prefix):])
			return false
		}

		delErr = txn.Del(key)
		if delErr != nil {
			return false
		}
		lvalue := &Value{}
		DecodeValue(value, lvalue)
		ret = append(ret, lvalue.Value)
		if len(ret) >= count {
			if !l.Complex {
				l.RIndex -= float64(count)
				return false
			}
		}
		return true
	})
	if delErr != nil {
		return nil, delErr
	}
	if err != nil {
		return nil, err
	}
	l.Length -= uint64(len(ret))
	if opt.count == 0 && len(ret) == 1 {
		return ret[0], nil
	}
	return ret, err
}

func (c *Command) BListHandle(txn *store.Txn, args [][]byte, lFunc ListFunc, opt *lOpt) (interface{}, error) {
	utils.ZapLog.Debug("BListHandle", zap.ByteStrings("args", args), zap.Any("opt", opt))
	object := NewObject(txn, BListType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	var change ChangeType
	if err == store.KeyNotFound {
		if opt.exist {
			return nil, store.KeyNotFound
		}
		id, err := uuid.NewUUID()
		if err != nil {
			return nil, err
		}
		object.Value = id[:]
		opt.l = &ListObject{}
		change = PlusCount
	} else if err != nil {
		return nil, err
	} else {
		l, err := DecodeListObjectExtra(object.Extra)
		if err != nil {
			return nil, err
		}
		if l.Length == 0 && opt.exist {
			err = DeleteKey(txn, key, object, 0, MinusCount)
			if err != nil {
				return nil, err
			}
			return nil, store.KeyNotFound
		}
		opt.l = l
	}
	var msgs [][]byte
	if len(args) > 1 {
		msgs = args[1:]
	} else {
		msgs = nil
	}
	ret, err := lFunc(txn, object, msgs, opt)
	if err != nil {
		return nil, err
	}
	if opt.readonly {
		return ret, nil
	}
	if opt.l.Length == 0 {
		err = setTxnObject(txn, key, object, MinusCount)
		if err != nil {
			return nil, err
		}
		return ret, nil
	}
	object.Timestamp = txn.Timestamp
	object.Extra = EncodeListObjectExtra(opt.l)
	err = setTxnObject(txn, key, object, change)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
