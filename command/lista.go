package command

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/google/uuid"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const (
	MaxListaPerTxn = 1000
)

var (
	AListFuncs = map[string]ListFunc{
		LINDEX_COMMAND:  alIndex,
		LINSERT_COMMAND: alInsert,
		LPOP_COMMAND: func(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
			return aPop(txn, object, args, opt, true)
		},
		LPUSH_COMMAND: func(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
			return aPush(txn, object, args, opt, true)
		},
		RPOP_COMMAND: func(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
			return aPop(txn, object, args, opt, false)
		},
		RPUSH_COMMAND: func(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
			return aPush(txn, object, args, opt, false)
		},
		LRANGE_COMMAND: func(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
			values, err := alRange(txn, object, args, opt, false)
			return values, err
		},
		LLEN_COMMAND:  alLen,
		LTRIM_COMMAND: alTrim,
		// LINFO_COMMAND:,
		LREM_COMMAND: alRem,
		LSET_COMMAND: alSet,
		LPOS_COMMAND: alPos,
	}
)

func nextLeftKey(txn *store.Txn, object *Object) []byte {
	key := make([]byte, 8+2)
	binary.BigEndian.PutUint64(key, utils.EncodeInt64ToCmpUint(-int64(txn.Timestamp)))
	binary.BigEndian.PutUint16(key[8:], utils.EncodeInt16ToCmpUint(-int16(txn.ListLID)))
	txn.ListLID++
	utils.ZapLog.Debug("nextLeftKey", zap.ByteString("key", key))
	return object.GetValueBytes(key)
}

func nextRightKey(txn *store.Txn, object *Object) []byte {
	key := make([]byte, 8+2)
	binary.BigEndian.PutUint64(key, utils.EncodeInt64ToCmpUint(int64(txn.Timestamp)))
	binary.BigEndian.PutUint16(key[8:], utils.EncodeInt16ToCmpUint(int16(txn.ListRID)))
	txn.ListRID++
	utils.ZapLog.Debug("nextRightKey", zap.ByteString("key", key))
	return object.GetValueBytes(key)
}

func alLen(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	count, err := GetCountByObject(txn, object)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(count), nil
}

func alRange(txn *store.Txn, object *Object, args [][]byte, opt *lOpt, needKey bool) ([][]byte, error) {
	startIndex := opt.index[0]
	startRevered := false
	if startIndex < 0 {
		startRevered = true
		startIndex = -startIndex - 1
	}
	endIndex := opt.index[1]
	endRevered := false
	if endIndex < 0 {
		endRevered = true
		endIndex = -endIndex - 1
	}
	if !startRevered && !endRevered && startIndex > endIndex {
		// return nil, xerror.ErrStartGreaterThanEnd
		return EmptyBytes, nil
	}
	if startRevered && endRevered && startIndex < endIndex {
		// return nil, xerror.ErrStartGreaterThanEnd
		return EmptyBytes, nil
	}
	prefix := object.GetValueBytes(nil)
	start := prefix
	end := utils.PrefixNext(prefix)
	lr := store.LeftRight{Prefix: prefix}
	var leftStart, leftEnd, rightStart, rightEnd int64
	if !startRevered {
		leftStart = startIndex
		rightEnd = math.MaxInt64
	} else {
		leftStart = 0
		rightEnd = startIndex
	}
	if !endRevered {
		leftEnd = endIndex
		rightStart = 0
	} else {
		leftEnd = math.MaxInt64
		rightStart = endIndex
	}

	if !startRevered || !endRevered {
		utils.ZapLog.Debug("alRange", zap.ByteString("start", start), zap.ByteString("end", end))
		iter, err := txn.Iter(start, end, false)
		if err != nil {
			return nil, err
		}
		lr.Left = iter
	}

	if startRevered || endRevered {
		utils.ZapLog.Debug("alRange reversed", zap.ByteString("start", start), zap.ByteString("end", end))
		iter, err := txn.Iter(start, end, true)
		if err != nil {
			return nil, err
		}
		lr.Right = iter
	}
	var leftC, rightC int64 = -1, -1
	leftList := make([][]byte, 0)
	rightList := make([][]byte, 0)
	var lastLeft, checkLeft []byte
	var lastRight, checkRight []byte
	meet := false
	for {
		left, right, err := lr.Next()
		if err != nil {
			return nil, err
		}
		if left == nil && right == nil {
			break
		}
		if len(left) > 0 {
			checkLeft = left[0]
		} else {
			checkLeft = lastLeft
		}

		if len(right) > 0 {
			checkRight = right[0]
		} else {
			checkRight = lastRight
		}

		if checkRight != nil && bytes.Compare(checkLeft, checkRight) > 0 {
			meet = true
			break
		}
		if checkRight != nil && bytes.Equal(checkLeft, checkRight) {
			meet = true
			// clear
			right = nil
			lr.Left = nil
			lr.Right = nil
		}

		if len(left) > 0 {
			leftC++
			lastLeft = left[0]
			utils.ZapLog.Debug("lRange left", zap.Int64("start", leftStart),
				zap.Int64("end", leftEnd), zap.Int64("current", leftC),
				zap.ByteString("key", left[0]))
			if leftC >= leftStart && leftC <= leftEnd {
				if !needKey {
					lvalue := &Value{}
					DecodeValue(left[1], lvalue)
					leftList = append(leftList, lvalue.Value)
				} else {
					leftList = append(leftList, left[0])
				}
			}

			if leftC >= leftEnd {
				lr.Left = nil
			}
		}

		if len(right) > 0 {
			rightC++
			lastRight = right[0]
			utils.ZapLog.Debug("lRange right", zap.Int64("start", rightStart),
				zap.Int64("end", rightEnd), zap.Int64("current", rightC),
				zap.ByteString("key", right[0]))
			if rightC >= rightStart && rightC <= rightEnd {
				if !needKey {
					lvalue := &Value{}
					DecodeValue(right[1], lvalue)
					rightList = append(rightList, lvalue.Value)
				} else {
					rightList = append(rightList, right[0])
				}
			}
			if rightC >= rightEnd {
				lr.Right = nil
			}
		}
	}
	if !startRevered && !endRevered {
		return leftList, nil
	} else if startRevered && endRevered {
		utils.ReverseBytes(rightList)
		return rightList, nil
	}

	if !startRevered {
		if rightC < rightStart && leftC < leftStart {
			// return nil, xerror.ErrOutOfRange
			return EmptyBytes, nil
		}

		if rightC < rightStart {
			if int(rightStart-rightC) > len(leftList) {
				// return nil, xerror.ErrStartGreaterThanEnd
				return EmptyBytes, nil
			}
			return leftList[:len(leftList)-int(rightStart-rightC)+1], nil
		}
		if leftC < leftStart {
			if int(leftStart-leftC) > len(rightList) {
				// return nil, xerror.ErrStartGreaterThanEnd
				return EmptyBytes, nil
			}
			ret := rightList[int(leftStart-leftC)-1:]
			utils.ReverseBytes(ret)
			return ret, nil
		}
		utils.ReverseBytes(rightList)
		return append(leftList, rightList...), nil
	} else {
		if rightC == rightEnd && leftC == leftEnd {
			if meet && len(leftList) > 0 {
				return leftList[len(leftList)-1:], nil
			}
			// return nil, xerror.ErrStartGreaterThanEnd
			return EmptyBytes, nil
		}

		var retRight [][]byte
		if int(leftEnd-leftC) > len(rightList) {
			if needKey {
				return EmptyBytes, nil
			}
			retRight = rightList
		} else {
			retRight = rightList[len(rightList)-int(leftEnd-leftC):]
		}
		utils.ReverseBytes(retRight)

		var retLeft [][]byte
		if int(rightEnd-rightC) > len(leftList) {
			if needKey {
				return EmptyBytes, nil
			}
			retLeft = leftList
		} else {
			retLeft = leftList[len(leftList)-int(rightEnd-rightC):]
		}
		return append(retLeft, retRight...), nil
	}
}

func alTrim(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	startIndex := opt.index[0]
	startRevered := false
	if startIndex < 0 {
		startRevered = true
		startIndex = -startIndex - 1
	}
	endIndex := opt.index[1]
	endRevered := false
	if endIndex < 0 {
		endRevered = true
		endIndex = -endIndex - 1
	}

	deleteAll := false
	if !startRevered && !endRevered && startIndex > endIndex {
		deleteAll = true
	} else if startRevered && endRevered && startIndex < endIndex {
		deleteAll = true
	}

	if deleteAll {
		key := object.GetKeyBytes()
		err := DeleteKey(txn, key, object, txn.Now, MinusCount)
		if err != nil {
			return nil, err
		}
		return OK, nil
	}

	prefix := object.GetValueBytes(nil)
	start := prefix
	end := utils.PrefixNext(prefix)
	lr := store.LeftRight{Prefix: prefix}
	var leftStart, leftEnd, rightStart, rightEnd int64
	if !startRevered {
		if !endRevered {
			leftStart = startIndex
			leftEnd = endIndex
		} else {
			leftStart = startIndex
			leftEnd = math.MaxInt64
			rightStart = endIndex
			rightEnd = math.MaxInt64
		}
	} else {
		if !endRevered {
			leftEnd = endIndex
			leftStart = 0
			rightStart = 0
			rightEnd = startIndex
		} else {
			rightStart = endIndex
			rightEnd = startIndex
		}
	}

	if !startRevered || !endRevered {
		utils.ZapLog.Debug("alRange", zap.ByteString("start", start), zap.ByteString("end", end))
		iter, err := txn.Iter(start, end, false)
		if err != nil {
			return nil, err
		}
		lr.Left = iter
	}

	if startRevered || endRevered {
		utils.ZapLog.Debug("alRange reversed", zap.ByteString("start", start), zap.ByteString("end", end))
		iter, err := txn.Iter(start, end, true)
		if err != nil {
			return nil, err
		}
		lr.Right = iter
	}
	var leftC, rightC int64 = -1, -1
	var lastLeft, checkLeft []byte
	var lastRight, checkRight []byte
	for {
		left, right, err := lr.Next()
		if err != nil {
			return nil, err
		}
		if left == nil && right == nil {
			break
		}
		if len(left) > 0 {
			checkLeft = left[0]
		} else {
			checkLeft = lastLeft
		}

		if len(right) > 0 {
			checkRight = right[0]
		} else {
			checkRight = lastRight
		}

		if checkRight != nil && bytes.Compare(checkLeft, checkRight) > 0 {
			break
		}
		if checkRight != nil && bytes.Equal(checkLeft, checkRight) {
			// clear
			lr.Left = nil
			lr.Right = nil
		}

		if len(left) > 0 {
			leftC++
			lastLeft = left[0]
			utils.ZapLog.Debug("lRange left", zap.Int64("start", leftStart),
				zap.Int64("end", leftEnd), zap.Int64("current", leftC),
				zap.ByteString("key", lastLeft))
			if leftC < leftStart || leftC > leftEnd {
				_, err = PutOrDeleteKV(txn, object, lastLeft, nil, -1)
				if err != nil {
					return nil, err
				}
			} else if leftC >= leftStart && leftEnd == math.MaxInt64 {
				lr.Left = nil
			}
		}

		if len(right) > 0 {
			rightC++
			lastRight = right[0]
			utils.ZapLog.Debug("lRange right", zap.Int64("start", rightStart),
				zap.Int64("end", rightEnd), zap.Int64("current", rightC),
				zap.ByteString("key", lastRight))
			if rightC < rightStart || rightC > rightEnd {
				_, err = PutOrDeleteKV(txn, object, lastRight, nil, -1)
				if err != nil {
					return nil, err
				}
			} else if rightC >= rightStart && rightEnd == math.MaxInt64 {
				lr.Right = nil
			}
		}
	}
	return OK, nil
}

func aPush(txn *store.Txn, object *Object, args [][]byte, opt *lOpt, left bool) (interface{}, error) {
	var err error
	for _, msg := range args {
		var lkey []byte
		if left {
			lkey = nextLeftKey(txn, object)
		} else {
			lkey = nextRightKey(txn, object)
		}
		lvalue := &Value{
			Value:     msg,
			Timestamp: txn.Timestamp,
		}
		_, err = PutOrDeleteKV(txn, object, lkey, EncodeValue(lvalue), 1)
		if err != nil {
			return redcon.SimpleInt(0), err
		}
	}
	return redcon.SimpleInt(len(args)), err
}

func alRem(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	prefix := object.GetValueBytes(nil)
	start := prefix
	end := utils.PrefixNext(prefix)
	var count int
	if opt.count >= 0 {
		count = opt.count
	} else {
		count = -opt.count
		start, end = end, start
	}
	var c int
	var delErr error
	err := txn.List(start, end, opt.max, func(key, value []byte) bool {
		if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
			return false
		}

		lvalue := &Value{}
		DecodeValue(value, lvalue)
		if !bytes.Equal(args[1], lvalue.Value) {
			return true
		}
		c++
		_, delErr = PutOrDeleteKV(txn, object, key, nil, -1)
		if delErr != nil {
			return false
		}
		if count > 0 && c >= count {
			return false
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
	return redcon.SimpleInt(c), nil
}

func alPos(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	prefix := object.GetValueBytes(nil)
	start := prefix
	end := utils.PrefixNext(prefix)
	uindex := opt.index[0]
	var length int64
	var err error
	if uindex < 0 {
		uindex = -uindex
		start, end = end, start
		length, err = GetCountByObject(txn, object)
		if err != nil {
			return nil, err
		}
	}
	idxs := make([]redcon.SimpleInt, 0)
	v := args[0]
	var count, idx int64
	begin := false
	err = txn.List(start, end, opt.max, func(key, value []byte) bool {
		if len(key) < len(prefix) || !bytes.Equal(key[:len(prefix)], prefix) {
			return false
		}
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
		if opt.index[0] >= 0 {
			idxs = append(idxs, redcon.SimpleInt(idx-1))
		} else {
			idxs = append(idxs, redcon.SimpleInt(length-idx))
		}
		return len(idxs) < opt.count
	})
	if err != nil && err != store.ReachLimit {
		return nil, err
	}
	if opt.count == 0 {
		if len(idxs) == 0 {
			return nil, nil
		}
		return idxs[0], nil
	}
	return idxs, nil
}

func aPop(txn *store.Txn, object *Object, _ [][]byte, opt *lOpt, left bool) (interface{}, error) {
	count := opt.count
	if count == 0 {
		count = 1
	}

	prefix := object.GetValueBytes(nil)
	start := prefix
	end := utils.PrefixNext(prefix)
	var delErr error
	ret := make([][]byte, 0)
	if !left {
		start, end = end, start
	}
	err := txn.List(start, end, count+1, func(key, value []byte) bool {
		if len(key) < len(prefix) {
			return true
		}
		_, delErr = PutOrDeleteKV(txn, object, key, nil, -1)
		if delErr != nil {
			return false
		}
		lvalue := &Value{}
		DecodeValue(value, lvalue)
		ret = append(ret, lvalue.Value)
		return len(ret) < count
	})
	if delErr != nil {
		return nil, delErr
	}
	if err != nil {
		return nil, err
	}
	if opt.count == 0 {
		if len(ret) == 1 {
			return ret[0], nil
		}
		return nil, nil
	}
	return ret, nil
}

func aindex(txn *store.Txn, object *Object, opt *lOpt) ([]byte, []byte, error) {
	i := opt.index[0]
	var reversed bool
	if i < 0 {
		reversed = true
		i = -i - 1
	}
	index := uint64(i)
	if index >= uint64(opt.max) {
		return nil, nil, xerror.ErrOutOfRange
	}

	prefix := object.GetValueBytes(nil)
	start := prefix
	end := utils.PrefixNext(prefix)
	var count uint64
	var retK, retV []byte
	if reversed {
		start, end = end, start
	}
	err := txn.List(start, end, opt.max, func(key, value []byte) bool {
		if len(key) < len(prefix) {
			return true
		}
		if count == index {
			retK = key
			retV = value
			return false
		}
		count++
		return true
	})
	if err != nil {
		return nil, nil, err
	}
	if len(retK) == 0 {
		return nil, nil, xerror.ErrOutOfRange
	}
	return retK, retV, nil
}

func aindexValue(txn *store.Txn, object *Object, v []byte, opt *lOpt) ([]byte, []byte, []byte, error) {
	prefix := object.GetValueBytes(nil)
	start := prefix
	end := utils.PrefixNext(prefix)
	var prev, cur, next []byte
	found := false
	err := txn.List(start, end, opt.max, func(key, value []byte) bool {
		if len(key) < len(prefix) {
			return true
		}
		if found {
			// next
			next = key
			return false
		}
		prev = cur
		cur = key
		lvalue := &Value{}
		DecodeValue(value, lvalue)
		if bytes.Equal(v, lvalue.Value) {
			found = true
			needNext := opt.after
			return needNext
		}
		return true
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if !found {
		return nil, nil, nil, xerror.ErrNotFound
	}
	return prev, cur, next, nil
}

func alIndex(txn *store.Txn, object *Object, _ [][]byte, opt *lOpt) (interface{}, error) {
	_, value, err := aindex(txn, object, opt)
	if err == xerror.ErrOutOfRange {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	lvalue := &Value{}
	DecodeValue(value, lvalue)
	return lvalue.Value, nil
}

func alSet(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	lkey, _, err := aindex(txn, object, opt)
	if err != nil {
		return nil, err
	}
	lvalue := &Value{
		Value:     args[1],
		Timestamp: txn.Timestamp,
	}
	_, err = PutOrDeleteKV(txn, object, lkey, EncodeValue(lvalue), 1)
	if err != nil {
		return nil, err
	}
	return OK, nil
}

func alInsert(txn *store.Txn, object *Object, args [][]byte, opt *lOpt) (interface{}, error) {
	prefix := object.GetValueBytes(nil)
	prev, cur, next, err := aindexValue(txn, object, args[1], opt)
	if err == xerror.ErrNotFound {
		return redcon.SimpleInt(-1), nil
	}

	if err != nil {
		return nil, err
	}
	var lkey []byte
	if opt.before {
		if len(prev) == 0 {
			lkey = nextLeftKey(txn, object)
		} else {
			preIndex := utils.DecodeFloat(prev[len(prefix):])
			curIndex := utils.DecodeFloat(cur[len(prefix):])
			index, err := utils.MiddleFloat(preIndex, curIndex)
			if err != nil {
				return nil, err
			}
			lkey = object.GetValueBytes(utils.EncodeFloat(index))
		}
	} else {
		if len(next) == 0 {
			lkey = nextRightKey(txn, object)
		} else {
			curIndex := utils.DecodeFloat(cur[len(prefix):])
			nextIndex := utils.DecodeFloat(next[len(prefix):])
			index, err := utils.MiddleFloat(curIndex, nextIndex)
			if err != nil {
				return nil, err
			}
			lkey = object.GetValueBytes(utils.EncodeFloat(index))
		}
	}

	lvalue := &Value{
		Value:     args[2],
		Timestamp: txn.Timestamp,
	}
	_, err = PutOrDeleteKV(txn, object, lkey, EncodeValue(lvalue), 1)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(1), nil
}

func (c *Command) AListHandle(txn *store.Txn, args [][]byte, lFunc ListFunc, opt *lOpt) (interface{}, error) {
	utils.ZapLog.Debug("AListHandle", zap.ByteStrings("args", args), zap.Any("opt", opt))
	var msgs [][]byte
	if len(args) > 1 {
		msgs = args[1:]
	} else {
		msgs = nil
	}

	object := NewObject(txn.UserId, txn.DBId, AListType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	create := false
	if err == store.KeyNotFound {
		if opt.exist {
			return nil, store.KeyNotFound
		}
		if opt.readonly {
			return lFunc(txn, nil, msgs, opt)
		}
		id, err := uuid.NewUUID()
		if err != nil {
			return nil, err
		}
		object.Timestamp = txn.Timestamp
		object.Value = id[:]
		create = true
	} else if err != nil {
		return nil, err
	}
	ret, err := lFunc(txn, object, msgs, opt)
	if err != nil {
		return nil, err
	}
	if opt.readonly || !create {
		return ret, nil
	}
	err = setTxnObject(txn, key, object, PlusCount)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
