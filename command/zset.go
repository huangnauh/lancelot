package command

import (
	"bytes"
	"math"
	"strconv"
	"strings"

	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

const (
	BYSCORE = 0x01
	BYLEX   = 0x02
	BYRANK  = 0x04
)

var (
	StartMemberKey = []byte{'m'}
	EndMemberKey   = []byte{'n'}
)

type ZAddOption struct {
	Check   CheckType
	Changed bool
	Incr    bool
}

func checkZaddOption(args [][]byte) (*ZAddOption, int, error) {
	opt := &ZAddOption{}
	for i := 1; i < len(args); i++ {
		str := strings.ToLower(utils.B2S(args[i]))
		switch str {
		case NX:
			if opt.Check&(CheckLT|CheckExist|CheckGT) != 0 {
				return nil, i, xerror.ErrXXNXCompat
			}
			opt.Check |= CheckNotExist
		case XX:
			if opt.Check&CheckNotExist == CheckNotExist {
				return nil, i, xerror.ErrXXNXCompat
			}
			opt.Check |= CheckExist
		case GT:
			if opt.Check&(CheckLT|CheckNotExist) != 0 {
				return nil, i, xerror.ErrXXNXCompat
			}
			opt.Check |= CheckGT
		case LT:
			if opt.Check&(CheckGT|CheckNotExist) != 0 {
				return nil, i, xerror.ErrXXNXCompat
			}
			opt.Check |= CheckLT
		case CH:
			opt.Changed = true
		case INCR:
			opt.Incr = true
		default:
			return opt, i, nil
		}
	}
	return opt, 0, xerror.ErrSyntax
}

// ZINCRBY key increment member
func (c *Command) ZIncrByHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(ZINCRBY_COMMAND)
	}
	score, err := strconv.ParseFloat(utils.B2S(args[1]), 64)
	if err != nil {
		return txn.SetError(xerror.ErrInvalidFloat)
	}
	object, err := c.GetOrCreateUUIDObject(txn, ZsetType, args[0])
	if err != nil {
		return txn.SetError(err)
	}
	score, _, err = c.zadd(txn, object, args[2], score, &ZAddOption{Incr: true})
	if err != nil {
		return txn.SetError(err)
	}
	return score
}

func (c *Command) zadd(txn *store.Txn, object *Object, member []byte, score float64, opt *ZAddOption) (float64, int, error) {
	var delta int64
	var oldScore float64
	var count int
	zkey := object.GetKeyFieldBytes(EncodeMemberKey(member))
	v, err := txn.Get(zkey)
	if err == store.KeyNotFound {
		if opt.Check&CheckExist == CheckExist {
			return 0, 0, nil
		}
		count++
		delta = 1
	} else if err != nil {
		return 0, 0, err
	} else {
		if opt.Check&CheckNotExist == CheckNotExist {
			return 0, 0, nil
		}
		if opt.Changed {
			count++
		}
		zvalue := &Value{}
		err = DecodeValue(v, zvalue)
		if err != nil {
			return 0, 0, err
		}
		oldScore = utils.DecodeFloat(zvalue.Value)

		if opt.Check&CheckGT == CheckGT {
			if score <= oldScore {
				return oldScore, 0, nil
			}
		}
		if opt.Check&CheckLT == CheckLT {
			if score >= oldScore {
				return oldScore, 0, nil
			}
		}

		if opt.Incr {
			score += oldScore
		}
		if score == oldScore {
			return score, 0, nil
		}
	}
	// zvalue.Value = utils.EncodeFloat(score)
	// zvalue.Timestamp = txn.Timestamp
	_, err = c.PutZset(txn, object, zkey, member, score, oldScore, delta)
	if err != nil {
		return 0, 0, nil
	}
	return score, count, nil
}

func (c *Command) zrem(txn *store.Txn, object *Object, member []byte) (bool, error) {
	zkey := object.GetKeyFieldBytes(EncodeMemberKey(member))
	v, err := txn.Get(zkey)
	if err == store.KeyNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}
	zvalue := &Value{}
	err = DecodeValue(v, zvalue)
	if err != nil {
		return false, err
	}
	score := utils.DecodeFloat(zvalue.Value)
	// zvalue.Value = utils.EncodeFloat(score)
	// zvalue.Timestamp = txn.Timestamp
	_, err = c.PutZset(txn, object, zkey, member, score, 0, -1)
	if err != nil {
		return false, err
	}
	return true, nil
}

// ZREM key member [member ...]
func (c *Command) ZRemHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(ZREM_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, ZsetType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}

	count := 0
	for i := 1; i < len(args); i++ {
		ok, err := c.zrem(txn, object, args[i])
		if err != nil {
			return txn.SetError(err)
		}
		if ok {
			count++
		}
	}
	return redcon.SimpleInt(count)
}

// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
func (c *Command) ZAddHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(ZADD_COMMAND)
	}
	opt, i, err := checkZaddOption(args)
	if err != nil {
		return err
	}
	if len(args) < i+2 || (len(args)-i-2)%2 != 0 {
		return txn.SetWrongArgs(ZADD_COMMAND)
	}

	if opt.Incr && len(args) > i+2 {
		return txn.SetError(xerror.ErrSyntax)
	}
	members := make(map[string]float64)
	for ; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(utils.B2S(args[i]), 64)
		if err != nil {
			return txn.SetError(err)
		}
		members[utils.B2S(args[i+1])] = score
	}
	object, err := c.GetOrCreateUUIDObject(txn, ZsetType, args[0])
	if err != nil {
		return txn.SetError(err)
	}
	count := 0
	for member, score := range members {
		memb := utils.S2B(member)
		_, c, err := c.zadd(txn, object, memb, score, opt)
		if err != nil {
			return txn.SetError(err)
		}
		count += c
	}
	return redcon.SimpleInt(count)
}

func EncodeMemberKey(member []byte) []byte {
	k := make([]byte, 1+len(member))
	k[0] = 'm'
	copy(k[1:], member)
	return k
}

func EncodeScoreKey(score float64, member []byte) []byte {
	k := make([]byte, 1+8+len(member))
	k[0] = 's'
	utils.EncodeFloatBytes(score, k[1:])
	copy(k[9:], member)
	return k
}

func (c *Command) PutZset(txn *store.Txn, object *Object, zkey, member []byte,
	score, oldScore float64, delta int64) (int64, error) {
	var err error
	if delta >= 0 {
		v := EncodeValue(&Value{
			Value:     utils.EncodeFloat(score),
			Timestamp: txn.Timestamp,
		})
		err = txn.Put(zkey, v)
	} else {
		err = txn.Del(zkey)
	}
	if err != nil {
		return 0, err
	}

	vkey := EncodeScoreKey(score, member)
	skey := object.GetKeyFieldBytes(vkey)
	if delta >= 0 {
		err = txn.Put(skey, EncodeValue(&Value{
			Timestamp: txn.Timestamp,
		}))
	} else {
		err = txn.Del(skey)
	}

	if err != nil {
		return 0, err
	}

	if delta == 0 {
		skey = object.GetKeyFieldBytes(EncodeScoreKey(oldScore, member))
		err = txn.Del(skey)
		return 0, err
	}

	count, err := c.GetCount(txn, txn.UserId, txn.DBId, object.Type, uint64(object.Hash), object.Value, member)
	if err == store.KeyNotFound {
	} else if err != nil {
		return 0, err
	}
	count.Value += delta
	err = c.SetCount(txn, count)
	return count.Value, err
}

// ZRANK key member
func (c *Command) ZRankHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(ZRANK_COMMAND)
	}
	ret, ok, err := c.zrank(txn, args, false)
	if err != nil {
		return txn.SetError(err)
	}
	if !ok {
		return nil
	}
	return redcon.SimpleInt(ret)
}

// ZREVRANK key member
func (c *Command) ZRevRankHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(ZREVRANK_COMMAND)
	}
	ret, ok, err := c.zrank(txn, args, true)
	if err != nil {
		return txn.SetError(err)
	}
	if !ok {
		return nil
	}
	return redcon.SimpleInt(ret)
}

func (c *Command) zrank(txn *store.Txn, args [][]byte, reversed bool) (int64, bool, error) {
	object := NewObject(txn.UserId, txn.DBId, ZsetType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return 0, false, nil
	} else if err != nil {
		return 0, false, err
	}
	zkey := object.GetKeyFieldBytes(EncodeMemberKey(args[1]))
	v, err := txn.Get(zkey)
	if err != store.KeyNotFound && err != nil {
		return 0, false, err
	}
	if v == nil {
		return 0, false, nil
	}

	prefix := object.GetKeyFieldBytes(nil)
	var count int64
	callback := func(k, v []byte) bool {
		if len(k) < len(prefix)+8+1 {
			return true
		}
		memb := k[len(prefix)+8+1:]
		if bytes.Equal(memb, args[1]) {
			return false
		}
		count++
		return count < int64(c.cfg.Key.ScanMaxCount)
	}
	err = c.ListByScore(txn, object, args[0], -math.MaxFloat64, math.MaxFloat64, true, true, callback, reversed)
	if err != nil {
		return 0, false, err
	}
	return count, true, nil
}

// ZMSCORE key member [member ...]
func (c *Command) ZMScoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(ZMSCORE_COMMAND)
	}
	ret, err := c.zscore(txn, args)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// ZSCORE key member
func (c *Command) ZScoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(ZSCORE_COMMAND)
	}
	ret, err := c.zscore(txn, args)
	if err != nil {
		return txn.SetError(err)
	}
	if len(ret) == 0 {
		return nil
	}
	return ret[0]
}

func (c *Command) zscore(txn *store.Txn, args [][]byte) ([]interface{}, error) {
	object := NewObject(txn.UserId, txn.DBId, ZsetType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	ret := make([]interface{}, len(args)-1)
	for i := 1; i < len(args); i++ {
		zkey := object.GetKeyFieldBytes(EncodeMemberKey(args[i]))
		v, err := txn.Get(zkey)
		if err != store.KeyNotFound && err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		zvalue := &Value{}
		err = DecodeValue(v, zvalue)
		if err != nil {
			return nil, err
		}
		ret[i-1] = utils.DecodeFloat(zvalue.Value)
	}
	return ret, nil
}

// ZCARD key
func (c *Command) ZCardHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(ZCARD_COMMAND)
	}
	ret, err := c.GetCountByKey(txn, args[0], ZsetType)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(ret)
}

func checkMinMaxScore(argMin, argMax []byte) (float64, float64, bool, bool, error) {
	var err error
	var min, max float64
	includeMin := true
	includeMax := true
	str := strings.ToLower(utils.B2S(argMin))
	if str == "inf" || str == "+inf" {
		return min, max, includeMin, includeMax, xerror.ErrEmpty
	} else if str == "-inf" {
		min = -math.MaxFloat64
	} else if len(str) == 0 {
		return min, max, includeMin, includeMax, xerror.ErrInvalidFloat
	} else {
		if str[0] == '(' {
			includeMin = false
			str = str[1:]
		}
		min, err = strconv.ParseFloat(str, 64)
		if err != nil {
			return min, max, includeMin, includeMax, xerror.ErrInvalidFloat
		}
	}
	str = strings.ToLower(utils.B2S(argMax))
	if str == "-inf" {
		return min, max, includeMin, includeMax, xerror.ErrEmpty
	} else if str == "inf" || str == "+inf" {
		max = math.MaxFloat64
	} else {
		if str[0] == '(' {
			includeMax = false
			str = str[1:]
		}
		max, err = strconv.ParseFloat(str, 64)
		if err != nil {
			return min, max, includeMin, includeMax, xerror.ErrInvalidFloat
		}
	}
	return min, max, includeMin, includeMax, nil
}

func getScoreMember(k, v, prefix []byte, min, max float64,
	includeMin, includeMax bool) (float64, []byte, bool, bool) {
	if len(k) < len(prefix)+8+1 {
		return 0, nil, true, false
	}
	score := utils.DecodeFloat(k[len(prefix)+1:])
	memb := k[len(prefix)+8+1:]
	if score < min || score > max {
		return score, memb, false, false
	}
	if !includeMin && score == min {
		return score, memb, true, false
	}
	if !includeMax && score == max {
		return score, memb, false, false
	}
	return score, memb, true, true
}

// ZCOUNT key min max
func (c *Command) ZCountHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(ZCOUNT_COMMAND)
	}
	min, max, includeMin, includeMax, err := checkMinMaxScore(args[1], args[2])
	if err == xerror.ErrEmpty {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	object := NewObject(txn.UserId, txn.DBId, ZsetType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	prefix := object.GetKeyFieldBytes(nil)
	var count int64
	callback := func(k, v []byte) bool {
		_, _, ok, found := getScoreMember(k, v, prefix, min, max, includeMin, includeMax)
		if !found {
			return ok
		}
		count++
		return true
	}
	err = c.ListByScore(txn, object, args[0], min, max, includeMin, includeMax, callback, false)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(count)
}

func (c *Command) ListByScore(txn *store.Txn, object *Object, arg []byte, min, max float64,
	includeMin, includeMax bool, callback store.KVCallback, reversed bool) error {
	start := object.GetKeyFieldBytes(EncodeScoreKey(min, arg))
	if includeMax {
		max = max + 1
	}
	end := object.GetKeyFieldBytes(EncodeScoreKey(max, arg))
	if reversed {
		start, end = end, start
	}
	return txn.List(start, end, c.cfg.Key.ScanMaxCount, callback)
}

func (c *Command) ListByMember(txn *store.Txn, object *Object, arg []byte, min, max []byte,
	includeMin, includeMax bool, callback store.KVCallback, reversed bool) error {
	var start, end []byte
	if includeMin {
		min = utils.NextKey(min)
	}
	start = object.GetKeyFieldBytes(EncodeMemberKey(min))
	if len(max) == 0 {
		end = object.GetKeyFieldBytes(EndMemberKey)
	} else {
		if includeMax {
			max = utils.NextKey(max)
		}
		end = object.GetKeyFieldBytes(EncodeMemberKey(max))
	}
	if reversed {
		start, end = end, start
	}
	return txn.List(start, end, c.cfg.Key.ScanMaxCount, callback)
}

func (c *Command) checkLimit(args [][]byte) (int64, int64, error) {
	if len(args) < 2 || len(args) > 3 {
		return 0, 0, xerror.ErrSyntax
	}
	offset, err := utils.GetNonnegativeInt64(args[1])
	if err != nil {
		return 0, 0, xerror.ErrOffset
	}
	limit := int64(c.cfg.Key.ScanMaxCount)
	if len(args) == 3 {
		limit, err = utils.GetNonnegativeInt64(args[2])
		if err != nil {
			return 0, 0, xerror.ErrCountNegative
		}
	}
	return offset, limit, nil
}

// BZPOPMIN key [key ...] timeout
func (c *Command) BZPopMinHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(BZPOPMIN_COMMAND)
	}
	ret, err := c.BlockHandle(txn, args, func(txn *store.Txn, args [][]byte) (interface{}, error) {
		return c.zpopMany(txn, args, false)
	})
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// BZPOPMAX key [key ...] timeout
func (c *Command) BZPopMaxHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(BZPOPMAX_COMMAND)
	}
	ret, err := c.BlockHandle(txn, args, func(txn *store.Txn, args [][]byte) (interface{}, error) {
		return c.zpopMany(txn, args, true)
	})
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// ZPOPMIN key [count]
func (c *Command) ZPopMinHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 || len(args) > 2 {
		return txn.SetWrongArgs(ZPOPMIN_COMMAND)
	}
	limit := int64(1)
	if len(args) == 2 {
		limit, err := utils.GetNonnegativeInt64(args[1])
		if err != nil {
			return txn.SetError(xerror.ErrCountNegative)
		}
		if limit == 0 {
			return EmptyInterface
		}
	}
	ret, err := c.zpop(txn, args[0], limit, false)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// ZPOPMAX key [count]
func (c *Command) ZPopMaxHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 || len(args) > 2 {
		return txn.SetWrongArgs(ZPOPMAX_COMMAND)
	}
	limit := int64(1)
	if len(args) == 2 {
		limit, err := utils.GetNonnegativeInt64(args[1])
		if err != nil {
			return txn.SetError(xerror.ErrCountNegative)
		}
		if limit == 0 {
			return EmptyInterface
		}
	}
	ret, err := c.zpop(txn, args[0], limit, true)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func (c *Command) zpopMany(txn *store.Txn, args [][]byte, reversed bool) (interface{}, error) {
	var ret []interface{}
	var err error
	for i := 0; i < len(args); i++ {
		object := NewObject(txn.UserId, txn.DBId, ZsetType, args[i])
		key := object.GetKeyBytes()
		err = getTxnObject(txn, key, object, false)
		if err == store.KeyNotFound {
			continue
		} else if err != nil {
			return nil, err
		}
		ret, err = c.objectZpop(txn, object, args[i], 1, reversed)
		if err != nil {
			return nil, err
		}
		if len(ret) > 0 {
			err = c.ZremValues(txn, object, ret)
			if err != nil {
				return nil, err
			}
		}
		return ret, nil
	}
	return nil, nil
}

func (c *Command) zpop(txn *store.Txn, arg []byte, limit int64, reversed bool) ([]interface{}, error) {
	var err error
	object := NewObject(txn.UserId, txn.DBId, ZsetType, arg)
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return EmptyInterface, nil
	} else if err != nil {
		return nil, err
	}
	return c.objectZpop(txn, object, arg, limit, reversed)
}

func (c *Command) objectZpop(txn *store.Txn, object *Object, arg []byte, limit int64, reversed bool) ([]interface{}, error) {
	ret, err := c.objectZRangeByScore(txn, object, arg, -math.MaxFloat64, math.MaxFloat64, true, true, &zRangeOption{
		reversed:   reversed,
		limit:      limit,
		withScores: true,
		offset:     0,
	})
	if err != nil {
		return nil, err
	}

	err = c.ZremValues(txn, object, ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (c *Command) ZremValues(txn *store.Txn, object *Object, ret []interface{}) error {
	var err error
	for i := 0; i < len(ret); i += 2 {
		member := ret[i].([]byte)
		zkey := object.GetKeyFieldBytes(EncodeMemberKey(member))
		_, err = c.PutZset(txn, object, zkey, member, ret[i+1].(float64), 0, -1)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Command) ZaddValues(txn *store.Txn, object *Object, ret []interface{}) (int, error) {
	count := 0
	for i := 0; i < len(ret); i += 2 {
		_, c, err := c.zadd(txn, object, ret[i].([]byte), ret[i+1].(float64), &ZAddOption{})
		if err != nil {
			return count, err
		}
		count += c
	}
	return count, nil
}

type zRangeOption struct {
	offset     int64
	limit      int64
	withScores bool
	reversed   bool
	by         int
}

func (c *Command) checkZRangeOption(args [][]byte) (*zRangeOption, error) {
	opt := &zRangeOption{limit: int64(c.cfg.Key.ScanMaxCount), by: BYRANK}
	var err error
	for i := 0; i < len(args); i++ {
		str := strings.ToLower(utils.B2S(args[i]))
		switch str {
		case "withscores":
			opt.withScores = true
		case "limit":
			opt.offset, opt.limit, err = c.checkLimit(args[i:])
			if err != nil {
				return nil, err
			}
			i += 2
		case "rev":
			opt.reversed = true
		case "byscore":
			if opt.by&^BYSCORE != 0 {
				return nil, xerror.ErrSyntax
			}
			opt.by = BYSCORE
		case "bylex":
			if opt.by&^BYLEX != 0 {
				return nil, xerror.ErrSyntax
			}
			opt.by = BYLEX
		default:
			return nil, xerror.ErrSyntax
		}
	}
	return opt, nil
}

// ZREVRANGE key start stop [WITHSCORES]
func (c *Command) ZRevRangeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 || len(args) > 4 {
		return txn.SetWrongArgs(ZREVRANGE_COMMAND)
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToLower(utils.B2S(args[3])) != "withscores" {
			return txn.SetWrongArgs(ZREVRANGE_COMMAND)
		}
		withScores = true
	}
	min, max, includeMin, includeMax, err := checkMinMaxScore(args[2], args[1])
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.zrangeByScore(txn, args[0], max, min, includeMax, includeMin, &zRangeOption{
		withScores: withScores,
		reversed:   true,
		limit:      int64(c.cfg.Key.ScanMaxCount),
	})
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
func (c *Command) ZRangeStoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 4 {
		return txn.SetWrongArgs(ZRANGESTORE_COMMAND)
	}
	object, err := c.DeleteThenCreateUUIDObject(txn, SetType, args[0])
	if err != nil {
		return txn.SetError(err)
	}
	opt, err := c.checkZRangeOption(args[4:])
	if err != nil {
		return txn.SetError(err)
	}
	opt.withScores = true
	ret, err := c.zrange(txn, args[1:], opt)
	if err != nil {
		return txn.SetError(err)
	}
	count, err := c.ZaddValues(txn, object, ret)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(count)
}

// ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
func (c *Command) ZRangeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(ZRANGE_COMMAND)
	}
	opt, err := c.checkZRangeOption(args[3:])
	if err != nil {
		return txn.SetError(err)
	}
	if opt.limit == 0 {
		return EmptyInterface
	}
	ret, err := c.zrange(txn, args, opt)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func (c *Command) zrange(txn *store.Txn, args [][]byte, opt *zRangeOption) ([]interface{}, error) {
	var ret []interface{}
	var err error
	if opt.by == BYSCORE {
		var min, max float64
		var includeMin, includeMax bool
		if opt.reversed {
			min, max, includeMin, includeMax, err = checkMinMaxScore(args[2], args[1])
		} else {
			min, max, includeMin, includeMax, err = checkMinMaxScore(args[1], args[2])
		}
		if err == xerror.ErrEmpty {
			return EmptyInterface, nil
		}
		if err != nil {
			return nil, err
		}
		ret, err = c.zrangeByScore(txn, args[0], min, max, includeMin, includeMax, opt)
	} else if opt.by == BYLEX {
		var min, max string
		var includeMin, includeMax bool
		if opt.reversed {
			min, max, includeMin, includeMax, err = checkMinMaxLex(args[2], args[1])
		} else {
			min, max, includeMin, includeMax, err = checkMinMaxLex(args[1], args[2])
		}
		if err == xerror.ErrEmpty {
			return EmptyInterface, nil
		}
		if err != nil {
			return nil, err
		}
		ret, err = c.zrangeByLex(txn, args[0], min, max, includeMin, includeMax, opt)
	} else {
		var object *Object
		var start, end int64
		var ok bool
		object, start, end, ok, err = c.checkMinMaxRank(txn, args)
		if err != nil {
			return nil, err
		}
		if !ok {
			return EmptyInterface, nil
		}
		ret, err = c.objectZrangeByRank(txn, object, args[0], start, end, opt)
	}

	if err != nil {
		return nil, err
	}
	return ret, nil
}

// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
func (c *Command) ZRevRangeByScoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(ZREVRANGEBYSCORE_COMMAND)
	}
	min, max, includeMin, includeMax, err := checkMinMaxScore(args[2], args[1])
	if err == xerror.ErrEmpty {
		return EmptyInterface
	}
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.zrangeByScoreHandle(txn, args, min, max, includeMin, includeMax, true)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func (c *Command) ZRangeByScoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(ZRANGEBYSCORE_COMMAND)
	}
	min, max, includeMin, includeMax, err := checkMinMaxScore(args[1], args[2])
	if err == xerror.ErrEmpty {
		return EmptyInterface
	}
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.zrangeByScoreHandle(txn, args, min, max, includeMin, includeMax, false)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func (c *Command) zrangeByScoreHandle(txn *store.Txn, args [][]byte, min, max float64, includeMin, includeMax, reversed bool) ([]interface{}, error) {
	opt, err := c.checkZRangeOption(args[3:])
	if err != nil {
		return nil, err
	}
	if opt.limit == 0 {
		return EmptyInterface, nil
	}
	return c.zrangeByScore(txn, args[0], min, max, includeMin, includeMax, opt)
}

func (c *Command) zrangeByScore(txn *store.Txn, arg []byte, min, max float64,
	includeMin, includeMax bool, opt *zRangeOption) ([]interface{}, error) {
	object := NewObject(txn.UserId, txn.DBId, ZsetType, arg)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return EmptyInterface, nil
	} else if err != nil {
		return nil, err
	}
	return c.objectZRangeByScore(txn, object, arg, min, max, includeMin, includeMax, opt)
}

func (c *Command) objectZremRangeByRank(txn *store.Txn, object *Object, arg []byte, min, max int64) (int, error) {
	ret, err := c.objectZrangeByRank(txn, object, arg, min, max, &zRangeOption{withScores: true})
	if err != nil {
		return 0, err
	}
	err = c.ZremValues(txn, object, ret)
	if err != nil {
		return 0, err
	}
	return len(ret) / 2, nil
}

func (c *Command) objectZrangeByRank(txn *store.Txn, object *Object, arg []byte, min, max int64, opt *zRangeOption) ([]interface{}, error) {
	prefix := object.GetKeyFieldBytes(nil)
	var count int64
	ret := make([]interface{}, 0)
	callback := func(k, v []byte) bool {
		if len(k) < len(prefix)+8+1 {
			return true
		}
		if count >= min && count <= max {
			score := utils.DecodeFloat(k[len(prefix)+1:])
			memb := k[len(prefix)+8+1:]
			ret = append(ret, memb)
			if opt.withScores {
				ret = append(ret, score)
			}
		}
		count++
		if count > max && count > min {
			return false
		}
		return true
	}
	err := c.ListByScore(txn, object, arg, -math.MaxFloat64, math.MaxFloat64, true, true, callback, opt.reversed)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (c *Command) objectZRangeByScore(txn *store.Txn, object *Object, arg []byte, min, max float64,
	includeMin, includeMax bool, opt *zRangeOption) ([]interface{}, error) {
	prefix := object.GetKeyFieldBytes(nil)
	ret := make([]interface{}, 0)
	var count, cnt int64
	callback := func(k, v []byte) bool {
		score, memb, ok, found := getScoreMember(k, v, prefix, min, max, includeMin, includeMax)
		if !found {
			return ok
		}
		count++
		if count < opt.offset+1 {
			return true
		}
		ret = append(ret, memb)
		if opt.withScores {
			ret = append(ret, score)
		}
		cnt++
		return cnt < opt.limit
	}
	err := c.ListByScore(txn, object, arg, min, max, includeMin, includeMax, callback, opt.reversed)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func checkStr(arg []byte) (string, bool, error) {
	key := utils.B2S(arg)
	if len(key) < 2 {
		return key, false, xerror.ErrSyntax
	}
	include := false
	if key[0] == '(' {
	} else if key[0] == '[' {
		include = true
	} else if key == "-" {
		return "", false, nil
	} else if key == "+" {
		return "", false, nil
	} else {
		return key, false, xerror.ErrMinMaxString
	}
	return key[1:], include, nil
}

// ZLEXCOUNT key min max
func (c *Command) ZLexCountHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(ZLEXCOUNT_COMMAND)
	}
	min, max, includeMin, includeMax, err := checkMinMaxLex(args[1], args[2])
	if err == xerror.ErrEmpty {
		return redcon.SimpleInt(0)
	}
	if err != nil {
		return txn.SetError(err)
	}
	if max == "" && min == "" {
		count, err := c.GetCountByKey(txn, args[0], ZsetType)
		if err != nil {
			return txn.SetError(err)
		}
		return redcon.SimpleInt(count)
	}

	ret, err := c.zrangeByLexHandle(txn, args, min, max, includeMin, includeMax, false)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(len(ret))
}

// ZREMRANGEBYRANK key start stop
func (c *Command) ZRemRangeByRankHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(ZREMRANGEBYRANK_COMMAND)
	}

	object, start, end, ok, err := c.checkMinMaxRank(txn, args)
	if err != nil {
		return txn.SetError(err)
	}
	if !ok {
		return redcon.SimpleInt(0)
	}
	ret, err := c.objectZremRangeByRank(txn, object, args[0], start, end)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(ret)
}

func (c *Command) checkMinMaxRank(txn *store.Txn, args [][]byte) (*Object, int64, int64, bool, error) {
	start, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return nil, 0, 0, false, xerror.ErrNotInteger
	}
	end, err := strconv.ParseInt(utils.B2S(args[2]), 10, 64)
	if err != nil {
		return nil, 0, 0, false, xerror.ErrNotInteger
	}
	object := NewObject(txn.UserId, txn.DBId, ZsetType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return object, 0, 0, false, nil
	} else if err != nil {
		return object, 0, 0, false, err
	}
	count, err := c.GetCountByObject(txn, object)
	if err != nil {
		return object, 0, 0, false, err
	}
	if end < 0 {
		end = count + end
	}
	if end < 0 {
		return object, 0, 0, false, nil
	}

	if start < 0 {
		start = count + start
	}

	if start < 0 {
		start = 0
	}
	if start > end {
		return object, 0, 0, false, nil
	}
	return object, start, end, true, nil
}

// ZREMRANGEBYSCORE key min max
func (c *Command) ZRemRangeByScoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(ZREMRANGEBYSCORE_COMMAND)
	}
	min, max, includeMin, includeMax, err := checkMinMaxScore(args[1], args[2])
	if err == xerror.ErrEmpty {
		return redcon.SimpleInt(0)
	}
	if err != nil {
		return txn.SetError(err)
	}
	object := NewObject(txn.UserId, txn.DBId, ZsetType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.objectZRangeByScore(txn, object, args[0], min, max, includeMin, includeMax, &zRangeOption{
		offset: 0, limit: int64(c.cfg.Key.ScanMaxCount), withScores: true})
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(len(ret) / 2)
}

// ZREMRANGEBYLEX key min max
func (c *Command) ZRemRangeByLexHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(ZREMRANGEBYLEX_COMMAND)
	}
	min, max, includeMin, includeMax, err := checkMinMaxLex(args[1], args[2])
	if err == xerror.ErrEmpty {
		return redcon.SimpleInt(0)
	}
	if err != nil {
		return txn.SetError(err)
	}

	object := NewObject(txn.UserId, txn.DBId, ZsetType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.objectZrangeByLex(txn, object, args[0], min, max, includeMin, includeMax, &zRangeOption{
		offset: 0, limit: int64(c.cfg.Key.ScanMaxCount), withScores: true})
	if err != nil {
		return txn.SetError(err)
	}
	err = c.ZremValues(txn, object, ret)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(len(ret) / 2)
}

// ZREVRANGEBYLEX key max min [LIMIT offset count]
func (c *Command) ZRevRangeByLexHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(ZREVRANGEBYLEX_COMMAND)
	}
	min, max, includeMin, includeMax, err := checkMinMaxLex(args[2], args[1])
	if err == xerror.ErrEmpty {
		return EmptySlice
	}
	if err != nil {
		return txn.SetError(err)
	}

	ret, err := c.zrangeByLexHandle(txn, args, min, max, includeMin, includeMax, true)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// ZRANGEBYLEX key min max [LIMIT offset count]
func (c *Command) ZRangeByLexHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(ZRANGEBYLEX_COMMAND)
	}
	min, max, includeMin, includeMax, err := checkMinMaxLex(args[1], args[2])
	if err == xerror.ErrEmpty {
		return EmptySlice
	}
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.zrangeByLexHandle(txn, args, min, max, includeMin, includeMax, false)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func checkMinMaxLex(argMin, argMax []byte) (string, string, bool, bool, error) {
	min, includeMin, err := checkStr(argMin)
	if err != nil {
		return "", "", false, false, err
	}
	max, includeMax, err := checkStr(argMax)
	if err != nil {
		return "", "", false, false, err
	}
	if min != "" && max != "" {
		if min > max {
			return "", "", false, false, xerror.ErrEmpty
		}
		if min == max && (!includeMax || !includeMin) {
			return "", "", false, false, xerror.ErrEmpty
		}
	}
	return min, max, includeMin, includeMax, nil
}

func (c *Command) zrangeByLexHandle(txn *store.Txn, args [][]byte, min, max string, includeMin, includeMax, reversed bool) ([]interface{}, error) {
	var offset int64
	var err error
	limit := int64(c.cfg.Key.ScanMaxCount)
	if len(args) > 3 {
		offset, limit, err = c.checkLimit(args[3:])
		if err != nil {
			return nil, err
		}
		if limit == 0 {
			return EmptyInterface, nil
		}
	}
	return c.zrangeByLex(txn, args[0], min, max, includeMin, includeMax, &zRangeOption{
		offset: offset, limit: limit, reversed: reversed})
}

func (c *Command) zrangeByLex(txn *store.Txn, arg []byte, min, max string, includeMin, includeMax bool, opt *zRangeOption) ([]interface{}, error) {
	object := NewObject(txn.UserId, txn.DBId, ZsetType, arg)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return EmptyInterface, nil
	} else if err != nil {
		return nil, err
	}
	return c.objectZrangeByLex(txn, object, arg, min, max, includeMin, includeMax, opt)
}

func (c *Command) objectZrangeByLex(txn *store.Txn, object *Object, arg []byte, min, max string,
	includeMin, includeMax bool, opt *zRangeOption) ([]interface{}, error) {
	prefix := object.GetKeyFieldBytes(nil)
	ret := make([]interface{}, 0)
	var count, cnt int64
	callback := func(k, v []byte) bool {
		if len(k) < len(prefix)+1 {
			return true
		}
		count++
		if count < opt.offset+1 {
			return true
		}
		key := k[len(prefix)+1:]
		ret = append(ret, key)
		cnt++
		return cnt < opt.limit
	}
	err := c.ListByMember(txn, object, arg, utils.S2B(min), utils.S2B(max), includeMin, includeMax, callback, opt.reversed)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// ZDIFF numkeys key [key ...] [WITHSCORES]
func (c *Command) ZDiffHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(ZDIFF_COMMAND)
	}
	num, err := utils.GetPositiveInt(args[0])
	if err != nil {
		return txn.SetError(xerror.ErrNotPositiveInteger)
	}

	if len(args) != num+1 && len(args) != num+2 {
		return txn.SetWrongArgs(ZDIFF_COMMAND)
	}

	getType := OnlyKey
	if len(args) == num+2 {
		if strings.ToLower(utils.B2S(args[num+1])) != "withscores" {
			return txn.SetError(xerror.ErrSyntax)
		}
		getType = BothKV
	}

	ret, err := c.diff(txn, args, ZsetType, getType, nil)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

type zsetOptions struct {
	num     int
	weights []int
	getType int
}

func checkZsetOptions(args [][]byte) (*zsetOptions, error) {
	num, err := utils.GetPositiveInt(args[0])
	if err != nil {
		return nil, xerror.ErrNotPositiveInteger
	}

	if len(args) < num+1 {
		return nil, xerror.ErrSyntax
	}

	opt := &zsetOptions{getType: OnlyKey, num: num}
	var agg bool
	for i := 1; i < len(args); i++ {
		str := strings.ToLower(utils.B2S(args[i]))
		switch str {
		case "weights":
			if i+opt.num >= len(args) {
				return nil, xerror.ErrSyntax
			}
			opt.weights = make([]int, opt.num)
			for j := 0; j < opt.num; j++ {
				weight, err := strconv.Atoi(utils.B2S(args[i+j+1]))
				if err != nil {
					return nil, xerror.ErrSyntax
				}
				opt.weights[j] = weight
			}
			i += opt.num
		case "withscores":
			opt.getType = BothKV
		case "aggregate":
			if i+1 >= len(args) {
				return nil, xerror.ErrSyntax
			}
			str := strings.ToLower(utils.B2S(args[i+1]))
			switch str {
			case "sum":
				opt.getType |= SumAGG
			case "min":
				opt.getType |= MinAGG
			case "max":
				opt.getType |= MaxAGG
			default:
				return nil, xerror.ErrSyntax
			}
			agg = true
		}
	}
	if !agg {
		opt.getType |= SumAGG
	}
	return opt, nil
}

// ZINTER numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
func (c *Command) ZInterHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(ZINTER_COMMAND)
	}
	opt, err := checkZsetOptions(args)
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.inter(txn, args[1:opt.num+1], ZsetType, opt.getType, opt.weights)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// ZINTERCARD numkeys key [key ...]
func (c *Command) ZInterCardHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(ZINTERCARD_COMMAND)
	}
	opt, err := checkZsetOptions(args)
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.inter(txn, args[1:opt.num+1], ZsetType, OnlyKey, nil)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(len(ret))
}

// ZUNION numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
func (c *Command) ZUnionHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(ZUNION_COMMAND)
	}
	opt, err := checkZsetOptions(args)
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.union(txn, args[1:opt.num+1], ZsetType, opt.getType, opt.weights)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// ZDIFFSTORE destination numkeys key [key ...]
func (c *Command) ZDiffStoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(ZDIFFSTORE_COMMAND)
	}
	return c.zstore(txn, args, c.diff)
}

// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
func (c *Command) ZInterStoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(ZINTERSTORE_COMMAND)
	}
	return c.zstore(txn, args, c.inter)
}

// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
func (c *Command) ZUnionStoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(ZUNIONSTORE_COMMAND)
	}
	return c.zstore(txn, args, c.union)
}

func (c *Command) zstore(txn *store.Txn, args [][]byte, sfunc SFunc) interface{} {
	opt, err := checkZsetOptions(args[1:])
	if err != nil {
		return txn.SetError(err)
	}
	opt.getType |= BothKV
	object, err := c.DeleteThenCreateUUIDObject(txn, SetType, args[0])
	if err != nil {
		return txn.SetError(err)
	}

	ret, err := sfunc(txn, args[2:opt.num+2], ZsetType, opt.getType, opt.weights)
	if err != nil {
		return txn.SetError(err)
	}
	count, err := c.ZaddValues(txn, object, ret)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(count)
}

// ZSCAN key cursor [MATCH pattern] [COUNT count]
func (c *Command) ZScanHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(ZSCAN_COMMAND)
	}
	return c.TypeScan(txn, ZsetType, args, BothKV)
}
