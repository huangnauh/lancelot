package command

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
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

type ListFunc func(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error)

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
		return 0, 0, xerror.ErrOutOfRange
	}
	if endIndex >= int64(l.Length) {
		endIndex = int64(l.Length) - 1
	}
	if startIndex > endIndex {
		return 0, 0, xerror.ErrStartGreaterThanEnd
	}
	return startIndex, endIndex, nil
}

func lTrim(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error) {
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
	if endIndex >= int64(l.Length)-1 {
		l.Length = uint64(endIndex - startIndex + 1)
		return OK, nil
	}

	if !l.Complex {
		start = object.GetValueBytes(utils.EncodeFloat(float64(endIndex) + 1))
		err = txn.List(start, end, opt.max, func(key, value []byte) bool {
			if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
				return false
			}
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

func linfo(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error) {
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

func lRange(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error) {
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

func lPos(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error) {
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

func lIndex(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error) {
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

func llen(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error) {
	return redcon.SimpleInt(l.Length), nil
}

func lrem(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error) {
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

func lInsert(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error) {
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
			return lPush(txn, object, l, args[2:], opt)
		}
		index, err = utils.MiddleFloat(ids[0], ids[1])
	} else {
		if ids[1] == l.RIndex {
			return rPush(txn, object, l, args[2:], opt)
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

func lSet(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error) {
	index, err := index(txn, object, l, opt)
	if err != nil {
		return "", err
	}
	lkey := object.GetValueBytes(utils.EncodeFloat(index))
	lvalue := &Value{
		Value:     args[1],
		Timestamp: txn.Timestamp,
	}
	err = txn.Put(lkey, EncodeValue(lvalue))
	if err != nil {
		return "", err
	}
	return OK, nil
}

func lPush(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error) {
	var err error
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

func rPush(txn *store.Txn, object *Object, l *ListObject, args [][]byte, opt *lOpt) (interface{}, error) {
	var err error
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

func lPop(txn *store.Txn, object *Object, l *ListObject, _ [][]byte, opt *lOpt) (interface{}, error) {
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
	if opt.count == 0 && len(ret) == 1 {
		return ret[0], nil
	}
	return ret, nil
}

func rPop(txn *store.Txn, object *Object, l *ListObject, _ [][]byte, opt *lOpt) (interface{}, error) {
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

func (c *Command) checkMaxLen(args [][]byte) (int, error) {
	if len(args) > 0 {
		str := strings.ToLower(utils.B2S(args[4]))
		if str != "maxlen" || len(args) != 2 {
			return 0, xerror.ErrSyntax
		}
		max, err := utils.GetPositiveInt(args[5])
		if err != nil {
			return 0, err
		}
		return max, nil
	}
	return c.cfg.Key.ScanMaxCount, nil
}

//(list) LREM key count element [MAXLEN len]
func (c *Command) LRemHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(LINSERT_COMMAND)
	}
	opt := &lOpt{}
	var err error
	opt.count, err = strconv.Atoi(utils.B2S(args[1]))
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	opt.max, err = c.checkMaxLen(args[3:])
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.ListHandle(txn, args, lrem, opt)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LINSERT key BEFORE|AFTER pivot element [MAXLEN len]
func (c *Command) LInsertHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 4 {
		return txn.SetWrongArgs(LINSERT_COMMAND)
	}
	str := strings.ToLower(utils.B2S(args[1]))
	opt := &lOpt{exist: true}
	if str == "before" {
		opt.before = true
	} else if str == "after" {
		opt.after = true
	} else {
		return txn.SetError(xerror.ErrSyntax)
	}

	var err error
	opt.max, err = c.checkMaxLen(args[4:])
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.ListHandle(txn, args, lInsert, opt)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LINDEX key index [MAXLEN len]
func (c *Command) LIndexHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(LINDEX_COMMAND)
	}
	index, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	opt := &lOpt{index: []int64{index}, readonly: true, exist: true}
	opt.max, err = c.checkMaxLen(args[2:])
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.ListHandle(txn, args, lIndex, opt)
	if err == store.KeyNotFound {
		return nil
	}
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LSET key index element [MAXLEN len]
func (c *Command) LSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(LSET_COMMAND)
	}
	index, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	opt := &lOpt{index: []int64{index}, exist: true}
	opt.max, err = c.checkMaxLen(args[3:])
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.ListHandle(txn, args, lSet, opt)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) RPUSHX key element [element ...]
func (c *Command) RPushXHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(RPUSHX_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, rPush, &lOpt{exist: true})
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) RPUSH key element [element ...]
func (c *Command) RPushHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(RPUSH_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, rPush, &lOpt{})
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LPUSHX key element [element ...]
func (c *Command) LPushXHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(LPUSHX_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, lPush, &lOpt{exist: true})
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LPUSH key element [element ...]
func (c *Command) LPushHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(LPUSH_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, lPush, &lOpt{})
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func (c *Command) listMany(txn *store.Txn, args [][]byte, lfunc ListFunc) (interface{}, error) {
	opt := &lOpt{exist: true}
	utils.ZapLog.Debug("block list", zap.ByteStrings("args", args))
	for i := 0; i < len(args); i++ {
		ret, err := c.ListHandle(txn, [][]byte{args[i]}, lfunc, opt)
		if err == store.KeyNotFound {
			continue
		} else if err != nil {
			return nil, err
		}
		msg, ok := ret.([]byte)
		if !ok {
			return nil, nil
		}
		return [][]byte{args[i], msg}, nil
	}
	return nil, nil
}

//(list) BRPOP key [key ...] timeout
func (c *Command) BrPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(BRPOP_COMMAND)
	}
	bfunc := func(txn *store.Txn, args [][]byte) (interface{}, error) {
		return c.listMany(txn, args, rPop)
	}
	ret, err := c.BlockHandle(txn, args, bfunc)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
func (c *Command) BlMoveHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 5 {
		return txn.SetWrongArgs(BLMOVE_COMMAND)
	}
	sstr := strings.ToLower(utils.B2S(args[2]))
	if sstr != "left" && sstr != "right" {
		return txn.SetError(xerror.ErrSyntax)
	}
	dstr := strings.ToLower(utils.B2S(args[3]))
	if dstr != "left" && dstr != "right" {
		return txn.SetError(xerror.ErrSyntax)
	}
	var value interface{}
	var err error
	sargs := [][]byte{args[0], args[4]}
	if sstr == "left" {
		value, err = c.BlockHandle(txn, sargs, func(txn *store.Txn, args [][]byte) (interface{}, error) {
			return c.listMany(txn, sargs, lPop)
		})
	} else {
		value, err = c.BlockHandle(txn, sargs, func(txn *store.Txn, args [][]byte) (interface{}, error) {
			return c.listMany(txn, sargs, rPop)
		})
	}
	if err != nil {
		return txn.SetError(err)
	}
	msg, ok := value.([][]byte)
	if !ok {
		return nil
	}
	if len(msg) != 2 {
		return nil
	}
	if dstr == "left" {
		_, err = c.ListHandle(txn, [][]byte{args[1], msg[1]}, lPush, &lOpt{})
	} else {
		_, err = c.ListHandle(txn, [][]byte{args[1], msg[1]}, rPush, &lOpt{})
	}
	if err != nil {
		return txn.SetError(err)
	}
	return msg[1]
}

//(list) BRPOPLPUSH source destination timeout
func (c *Command) BRPopLPushHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(BRPOPLPUSH_COMMAND)
	}
	sargs := [][]byte{args[0], args[2]}
	bfunc := func(txn *store.Txn, args [][]byte) (interface{}, error) {
		return c.listMany(txn, args, rPop)
	}
	value, err := c.BlockHandle(txn, sargs, bfunc)
	if err != nil {
		return txn.SetError(err)
	}
	msg, ok := value.([][]byte)
	if !ok {
		return nil
	}
	if len(msg) != 2 {
		return nil
	}
	_, err = c.ListHandle(txn, [][]byte{args[1], msg[1]}, lPush, &lOpt{})
	if err != nil {
		return txn.SetError(err)
	}
	return msg[1]
}

//(list) BLPOP key [key ...] timeout
func (c *Command) BlPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(BLPOP_COMMAND)
	}
	bfunc := func(txn *store.Txn, args [][]byte) (interface{}, error) {
		return c.listMany(txn, args, lPop)
	}
	ret, err := c.BlockHandle(txn, args, bfunc)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) RPOP key [count]
func (c *Command) RPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(RPOP_COMMAND)
	}
	return c.pophandle(txn, args, rPop)
}

//(list) LPOP key [count]
func (c *Command) LPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(RPOP_COMMAND)
	}
	return c.pophandle(txn, args, lPop)
}

func (c *Command) pophandle(txn *store.Txn, args [][]byte, lfunc ListFunc) interface{} {
	opt := &lOpt{exist: true}
	if len(args) == 2 {
		count, err := utils.GetPositiveInt(args[1])
		if err == utils.ErrInvalidInt && count == 0 {
			return nil
		}
		if err != nil {
			return txn.SetError(xerror.ErrNotPositiveInteger)
		}
		opt.count = count
	}
	ret, err := c.ListHandle(txn, args, lfunc, opt)
	if err == store.KeyNotFound {
		return nil
	}
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// (list) LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
func (c *Command) LPosHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 || len(args)%2 != 0 {
		return txn.SetWrongArgs(LPOS_COMMAND)
	}
	opt := &lOpt{count: 0, max: c.cfg.Key.ScanMaxCount, index: []int64{0}, exist: true, readonly: true}
	var err error
	for i := 2; i < len(args); i += 2 {
		switch strings.ToLower(utils.B2S(args[i])) {
		case "rank":
			index, err := strconv.ParseInt(utils.B2S(args[i+1]), 10, 64)
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
			if index == 0 {
				return txn.SetError(xerror.ErrRankZero)
			}
			opt.index = []int64{index}
		case "count":
			opt.count, err = strconv.Atoi(utils.B2S(args[i+1]))
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
			if opt.count < 0 {
				return txn.SetError(xerror.ErrCountNegative)
			}
			if opt.count == 0 {
				opt.count = math.MaxInt64
			}
		case "maxlen":
			opt.max, err = utils.GetPositiveInt(args[i+1])
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
		default:
			return txn.SetWrongArgs(LPOS_COMMAND)
		}
	}

	ret, err := c.ListHandle(txn, args, lPos, opt)
	if err == store.KeyNotFound || err == store.ReachLimit {
		if opt.count > 0 {
			return []redcon.SimpleInt{}
		}
		return nil
	}
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func (c *Command) getStartEnd(args [][]byte) (*lOpt, error) {
	opt := &lOpt{count: 0, max: c.cfg.Key.ScanMaxCount}
	start, err := strconv.ParseInt(utils.B2S(args[0]), 10, 64)
	if err != nil {
		return opt, xerror.ErrNotInteger
	}
	end, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return opt, xerror.ErrNotInteger
	}
	opt.index = []int64{start, end}
	opt.max, err = c.checkMaxLen(args[2:])
	if err != nil {
		return opt, err
	}
	return opt, nil
}

// (list) LTRIM key start stop [MAXLEN len]
func (c *Command) LTrimHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(LTRIM_COMMAND)
	}
	opt, err := c.getStartEnd(args[1:])
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.ListHandle(txn, args, lTrim, opt)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// (list) LINFO key [start end]
func (c *Command) LInfoHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 3 {
		return txn.SetWrongArgs(LINFO_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, linfo, &lOpt{exist: true, max: c.cfg.Key.ScanMaxCount})
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// (list) LRANGE key start stop [MAXLEN len]
func (c *Command) LRangeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(LRANGE_COMMAND)
	}
	opt, err := c.getStartEnd(args[1:])
	if err != nil {
		return txn.SetError(err)
	}
	opt.exist = true
	opt.readonly = true
	ret, err := c.ListHandle(txn, args, lRange, opt)
	if err == store.KeyNotFound {
		return []redcon.SimpleInt{}
	}
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LLEN key
func (c *Command) LLenHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(LLEN_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, llen, &lOpt{exist: true, readonly: true})
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	}
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// (list) LMOVE source destination LEFT|RIGHT LEFT|RIGHT
func (c *Command) LMoveHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 4 {
		return txn.SetWrongArgs(LMOVE_COMMAND)
	}
	sstr := strings.ToLower(utils.B2S(args[2]))
	if sstr != "left" && sstr != "right" {
		return txn.SetError(xerror.ErrSyntax)
	}
	dstr := strings.ToLower(utils.B2S(args[3]))
	if dstr != "left" && dstr != "right" {
		return txn.SetError(xerror.ErrSyntax)
	}
	var value interface{}
	var err error
	if sstr == "left" {
		value, err = c.ListHandle(txn, [][]byte{args[0]}, lPop, &lOpt{exist: true})
	} else {
		value, err = c.ListHandle(txn, [][]byte{args[0]}, rPop, &lOpt{exist: true})
	}
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	msg, ok := value.([]byte)
	if !ok {
		return nil
	}
	if dstr == "left" {
		_, err = c.ListHandle(txn, [][]byte{args[1], msg}, lPush, &lOpt{})
	} else {
		_, err = c.ListHandle(txn, [][]byte{args[1], msg}, rPush, &lOpt{})
	}
	if err != nil {
		return txn.SetError(err)
	}
	return value
}

//(list) RPOPLPUSH source destination
func (c *Command) RPopLPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(RPOPLPUSH_COMMAND)
	}
	value, err := c.ListHandle(txn, [][]byte{args[0]}, rPop, &lOpt{exist: true})
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	msg, ok := value.([]byte)
	if !ok {
		return nil
	}
	_, err = c.ListHandle(txn, [][]byte{args[1], msg}, lPush, &lOpt{})
	if err != nil {
		return txn.SetError(err)
	}
	return value
}

func (c *Command) ListHandle(txn *store.Txn, args [][]byte, lFunc ListFunc, opt *lOpt) (interface{}, error) {
	utils.ZapLog.Debug("ListHandle", zap.ByteStrings("args", args), zap.Any("opt", opt))
	object := NewObject(txn.UserId, txn.DBId, ListType, args[0])
	key := object.GetKeyBytes()
	err := c.getTxnObject(txn, key, object, true)
	var l *ListObject
	if err == store.KeyNotFound {
		if opt.exist {
			return nil, store.KeyNotFound
		}
		id, err := uuid.NewUUID()
		if err != nil {
			return nil, err
		}
		object.Value = id[:]
		l = &ListObject{}
	} else if err != nil {
		return nil, err
	} else {
		l, err = DecodeListObjectExtra(object.Extra)
		if err != nil {
			return nil, err
		}
		if l.Length == 0 && opt.exist {
			err = c.DeleteKey(txn, key, object, 0, MinusCount)
			if err != nil {
				return nil, err
			}
			return nil, store.KeyNotFound
		}
	}
	var msgs [][]byte
	if len(args) > 1 {
		msgs = args[1:]
	} else {
		msgs = nil
	}
	ret, err := lFunc(txn, object, l, msgs, opt)
	if err != nil {
		return nil, err
	}
	if opt.readonly {
		return ret, nil
	}
	if l.Length == 0 {
		err = c.DeleteKey(txn, key, object, 0, MinusCount)
		if err != nil {
			return nil, err
		}
		return ret, nil
	}
	object.Timestamp = txn.Timestamp
	object.Extra = EncodeListObjectExtra(l)
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return nil, err
	}
	return ret, nil
}
