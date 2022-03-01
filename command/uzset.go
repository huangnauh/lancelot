package command

import (
	"encoding/binary"

	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
)

//TODO:
func EncodeUint64Key(score uint64, member []byte) []byte {
	k := make([]byte, 1+8+len(member))
	k[0] = 's'
	binary.BigEndian.PutUint64(k[1:], score)
	copy(k[9:], member)
	return k
}

func EncodeUint64ScoreNext(score uint64) []byte {
	k := make([]byte, 1+8)
	k[0] = 's'
	binary.BigEndian.PutUint64(k[1:], score)
	return utils.PrefixNext(k)
}

func (c *Command) PutUzset(txn *store.Txn, object *Object, zkey, member []byte,
	score, oldScore uint64, delta int64) (int64, error) {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, score)
	var err error
	if delta >= 0 {
		v := EncodeValue(&Value{
			Value:     value,
			Timestamp: txn.Timestamp,
		})
		err = txn.Put(zkey, v)
	} else {
		err = txn.Del(zkey)
	}
	if err != nil {
		return 0, err
	}

	vkey := EncodeUint64Key(score, member)
	skey := object.GetValueBytes(vkey)
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
		skey = object.GetValueBytes(EncodeUint64Key(oldScore, member))
		err = txn.Del(skey)
		return 0, err
	}

	count, err := GetCount(txn, txn.UserId, txn.DBId, uint64(object.Hash), KeyPrefix, object.Value, member)
	if err == store.KeyNotFound {
	} else if err != nil {
		return 0, err
	}
	count.Value += delta
	err = SetCount(txn, count)
	return count.Value, err
}

func (c *Command) uzadd(txn *store.Txn, object *Object, member []byte, score uint64, opt *checkOption) (uint64, int, error) {
	var delta int64
	var oldScore uint64
	var count int
	utils.ZapLog.Debug("uzadd", zap.String("member", utils.B2S(member)), zap.Uint64("score", score))
	zkey := object.GetValueBytes(EncodeMemberKey(member))
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
		zvalue := &Value{}
		err = DecodeValue(v, zvalue)
		if err != nil {
			return 0, 0, err
		}
		oldScore = binary.BigEndian.Uint64(zvalue.Value)
		if opt.Incr {
			score += oldScore
		}

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

		if score == oldScore {
			return score, 0, nil
		}
		if opt.Changed {
			count++
		}
	}

	// zvalue.Value = utils.EncodeFloat(score)
	// zvalue.Timestamp = txn.Timestamp
	_, err = c.PutUzset(txn, object, zkey, member, score, oldScore, delta)
	if err != nil {
		return 0, 0, err
	}
	return score, count, nil
}

func (c *Command) uzaddMembers(txn *store.Txn, key []byte, typo ObjectType, members map[string]uint64, opt *checkOption) interface{} {
	object, err := c.GetOrCreateUUIDObject(txn, typo, key)
	if err != nil {
		return txn.SetError(err)
	}
	var s uint64
	count := 0
	for member, score := range members {
		memb := utils.S2B(member)
		score, c, err := c.uzadd(txn, object, memb, score, opt)
		if err != nil {
			return txn.SetError(err)
		}
		count += c
		s = score
	}
	if !opt.Incr {
		return redcon.SimpleInt(count)
	}
	if count == 0 {
		return nil
	}
	return s
}

func (c *Command) ListByUint64Score(txn *store.Txn, object *Object, arg []byte, min, max uint64,
	includeMin, includeMax bool, callback store.KVCallback, reversed bool) error {
	utils.ZapLog.Debug("ListByScore", zap.String("arg", utils.B2S(arg)), zap.Uint64("min", min),
		zap.Uint64("max", max), zap.Bool("includeMin", includeMin), zap.Bool("includeMax", includeMax),
		zap.Bool("reversed", reversed))
	start := object.GetValueBytes(EncodeUint64Key(min, nil))
	end := object.GetValueBytes(EncodeUint64ScoreNext(max))
	if reversed {
		start, end = end, start
	}
	return txn.List(start, end, txn.Config.Redis.ScanMaxCount, callback)
}

func (c *Command) zgetMember(txn *store.Txn, object *Object, m []byte) (*Value, error) {
	zkey := object.GetValueBytes(EncodeMemberKey(m))
	v, err := txn.Get(zkey)
	if err == store.KeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	zvalue := &Value{}
	err = DecodeValue(v, zvalue)
	if err != nil {
		return nil, err
	}
	return zvalue, nil
}

func (c *Command) zget(txn *store.Txn, typo ObjectType, k, m []byte) (*Object, *Value, error) {
	object := c.NewObject(txn, typo, k)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil, nil, nil
	} else if err != nil {
		return nil, nil, err
	}
	zvalue, err := c.zgetMember(txn, object, m)
	if err != nil {
		return nil, nil, err
	}
	return object, zvalue, nil
}

func (c *Command) objectZRangeByUint64Score(txn *store.Txn, object *Object, arg []byte, min, max uint64,
	includeMin, includeMax bool, opt *zRangeOption) ([]interface{}, error) {
	prefix := object.GetValueBytes(nil)
	ret := make([]interface{}, 0)
	var count, cnt int64
	callback := func(k, v []byte) bool {
		if len(k) < len(prefix)+8+1 {
			return false
		}
		score := binary.BigEndian.Uint64(k[len(prefix)+1:])
		memb := k[len(prefix)+8+1:]
		utils.ZapLog.Debug("score member", zap.Uint64("min", min), zap.Uint64("max", max),
			zap.Uint64("score", score), zap.String("member", utils.B2S(memb)))
		if score < min || score > max {
			return false
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
	err := c.ListByUint64Score(txn, object, arg, min, max, includeMin, includeMax, callback, opt.reversed)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
