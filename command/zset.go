package command

import (
	"math"
	"strconv"
	"strings"

	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
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
	var oldScore float64
	var delta int64
	for member, score := range members {
		memb := utils.S2B(member)
		zkey := object.GetKeyFieldBytes(EncodeMemberKey(memb))
		v, err := txn.Get(zkey)
		if err == store.KeyNotFound {
			if opt.Check&CheckExist == CheckExist {
				continue
			}
			count++
			delta = 1
		} else if err != nil {
			return txn.SetError(err)
		} else {
			if opt.Check&CheckNotExist == CheckNotExist {
				continue
			}
			if opt.Changed {
				count++
			}
			zvalue := &Value{}
			err = DecodeValue(v, zvalue)
			if err != nil {
				return txn.SetError(err)
			}
			oldScore = utils.DecodeFloat(zvalue.Value)

			if opt.Check&CheckGT == CheckGT {
				if score <= oldScore {
					continue
				}
			}
			if opt.Check&CheckLT == CheckLT {
				if score >= oldScore {
					continue
				}
			}

			if opt.Incr {
				score += oldScore
			}
			if score == oldScore {
				continue
			}
		}
		// zvalue.Value = utils.EncodeFloat(score)
		// zvalue.Timestamp = txn.Timestamp
		_, err = c.PutZset(txn, object, zkey, memb, score, oldScore, delta)
		if err != nil {
			return txn.SetError(err)
		}
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
	v := EncodeValue(&Value{
		Value:     utils.EncodeFloat(score),
		Timestamp: txn.Timestamp,
	})
	err := txn.Put(zkey, v)
	if err != nil {
		return 0, err
	}
	vkey := EncodeScoreKey(score, member)
	skey := object.GetKeyFieldBytes(vkey)
	err = txn.Put(skey, EncodeValue(&Value{
		Timestamp: txn.Timestamp,
	}))
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

func checkMinMaxScore(args [][]byte) (float64, float64, bool, bool, error) {
	if len(args) < 2 {
		return 0, 0, false, false, xerror.ErrSyntax
	}

	var err error
	var min, max float64
	includeMin := true
	includeMax := true
	str := strings.ToLower(utils.B2S(args[0]))
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
	str = strings.ToLower(utils.B2S(args[1]))
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
	min, max, includeMin, includeMax, err := checkMinMaxScore(args[1:])
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
	err = c.ListByScore(txn, object, args[0], min, max, includeMin, includeMax, callback)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(count)
}

func (c *Command) ListByScore(txn *store.Txn, object *Object, arg []byte, min, max float64,
	includeMin, includeMax bool, callback store.KVCallback) error {
	start := object.GetKeyFieldBytes(EncodeScoreKey(min, arg))
	if includeMax {
		max = max + 1
	}
	end := object.GetKeyFieldBytes(EncodeScoreKey(max, arg))
	return txn.List(start, end, c.cfg.Key.ScanMaxCount, callback)
}

func (c *Command) ListByMember(txn *store.Txn, object *Object, arg []byte, min, max []byte,
	includeMin, includeMax bool, callback store.KVCallback) error {
	if includeMin {
		min = utils.NextKey(min)
	}
	start := object.GetKeyFieldBytes(EncodeMemberKey(min))
	if includeMax {
		max = utils.NextKey(max)
	}
	end := object.GetKeyFieldBytes(EncodeMemberKey(max))
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

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func (c *Command) ZRangeByScoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(ZRANGEBYSCORE_COMMAND)
	}
	min, max, includeMin, includeMax, err := checkMinMaxScore(args[1:])
	if err != nil {
		return txn.SetError(err)
	}
	var withScores bool
	var offset int64
	limit := int64(c.cfg.Key.ScanMaxCount)
LOOP:
	for i := 3; i < len(args); i++ {
		str := strings.ToLower(utils.B2S(args[i]))
		switch str {
		case "withscores":
			withScores = true
		case "limit":
			offset, limit, err = c.checkLimit(args[i:])
			if err != nil {
				return txn.SetError(err)
			}
			if limit == 0 {
				return EmptySlice
			}
			break LOOP
		default:
			return txn.SetError(xerror.ErrSyntax)
		}
	}
	object := NewObject(txn.UserId, txn.DBId, ZsetType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return EmptySlice
	} else if err != nil {
		return txn.SetError(err)
	}
	prefix := object.GetKeyFieldBytes(nil)
	ret := make([]interface{}, 0)
	var count, cnt int64
	callback := func(k, v []byte) bool {
		score, memb, ok, found := getScoreMember(k, v, prefix, min, max, includeMin, includeMax)
		if !found {
			return ok
		}
		count++
		if count < offset+1 {
			return true
		}
		ret = append(ret, memb)
		if withScores {
			ret = append(ret, score)
		}
		cnt++
		return cnt < limit
	}
	err = c.ListByScore(txn, object, args[0], min, max, includeMin, includeMax, callback)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
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
	} else {
		return key, false, xerror.ErrMinMaxString
	}
	return key[1:], include, nil
}

// ZRANGEBYLEX key min max [LIMIT offset count]
func (c *Command) ZRangeByLexHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(ZRANGEBYLEX_COMMAND)
	}

	min, includeMin, err := checkStr(args[1])
	if err != nil {
		return txn.SetError(err)
	}
	max, includeMax, err := checkStr(args[2])
	if err != nil {
		return txn.SetError(err)
	}
	var offset int64
	limit := int64(c.cfg.Key.ScanMaxCount)
	if len(args) > 3 {
		offset, limit, err = c.checkLimit(args[3:])
		if err != nil {
			return txn.SetError(err)
		}
	}
	object := NewObject(txn.UserId, txn.DBId, ZsetType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return EmptySlice
	} else if err != nil {
		return txn.SetError(err)
	}
	prefix := object.GetKeyFieldBytes(nil)
	ret := make([][]byte, 0)
	var count, cnt int64
	callback := func(k, v []byte) bool {
		if len(k) < len(prefix)+1 {
			return true
		}
		count++
		if count < offset+1 {
			return true
		}
		key := k[len(prefix)+1:]
		ret = append(ret, key)
		cnt++
		return cnt < limit
	}
	err = c.ListByMember(txn, object, args[0], utils.S2B(min), utils.S2B(max), includeMin, includeMax, callback)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}
