package command

import (
	"bytes"
	"math/rand"
	"strconv"

	"github.com/huangnauh/lancelot/config"
	"github.com/huangnauh/lancelot/redcon"
	"github.com/huangnauh/lancelot/store"
	"github.com/huangnauh/lancelot/utils"
	"github.com/huangnauh/lancelot/xerror"
	"go.uber.org/zap"
)

type SFunc func(txn *store.Txn, args [][]byte, typo ObjectType, getType int, weight []float64) ([]interface{}, error)

// (sets) SMISMEMBER key member [member ...]
func (c *Command) SMIsMemberHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(SMISMEMBER_COMMAND)
	}
	object := NewObject(txn, SetType, args[0])
	key := object.GetKeyBytes()
	ret := make([]redcon.SimpleInt, len(args)-1)
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return ret
	} else if err != nil {
		return txn.SetError(err)
	}
	for i := 1; i < len(args); i++ {
		skey := object.GetValueBytes(args[i])
		_, err := txn.Get(skey)
		if err == store.KeyNotFound {
			continue
		}
		if err != nil {
			return txn.SetError(err)
		}
		ret[i-1] = redcon.SimpleInt(1)
	}
	return ret
}

// (sets) SISMEMBER key member
func (c *Command) SIsMemberHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(SISMEMBER_COMMAND)
	}
	object := NewObject(txn, SetType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	}
	if err != nil {
		return txn.SetError(err)
	}
	skey := object.GetValueBytes(args[1])
	_, err = txn.Get(skey)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	}
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(1)
}

// (sets) SADD key member [member ...]
func (c *Command) SAddHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(SADD_COMMAND)
	}
	count, err := c.sadd(txn, args[0], args[1:], false)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(count)
}

func (c *Command) sadd(txn *store.Txn, arg []byte, args [][]byte, check bool) (int64, error) {
	object, err := c.GetOrCreateUUIDObject(txn, SetType, arg)
	if err != nil {
		return 0, err
	}
	if check {
		return 0, nil
	}

	var count int64
	for i := 0; i < len(args); i++ {
		skey := object.GetValueBytes(args[i])
		_, err := txn.Get(skey)
		if err == store.KeyNotFound {
			count++
			svalue := &Value{Timestamp: txn.Timestamp}
			_, err = PutOrDeleteKV(txn, object, skey, EncodeValue(svalue), 1)
		}
		if err != nil {
			return 0, err
		}
		continue
	}

	return count, nil
}

// (sets) SREM key member [member ...]
func (c *Command) SRemHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(SREM_COMMAND)
	}
	count, err := c.srem(txn, args[0], args[1:])
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(count)
}

func (c *Command) srem(txn *store.Txn, arg []byte, args [][]byte) (int64, error) {
	object := NewObject(txn, SetType, arg)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var count int64
	for i := 0; i < len(args); i++ {
		skey := object.GetValueBytes(args[i])
		_, err := txn.Get(skey)
		if err == store.KeyNotFound {
			continue
		}
		if err != nil {
			return 0, err
		}
		count++
		_, err = PutOrDeleteKV(txn, object, skey, nil, -1)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

// SMOVE source destination member
func (c *Command) SMoveHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(SMOVE_COMMAND)
	}
	count, err := c.srem(txn, args[0], args[2:])
	if err != nil {
		return txn.SetError(err)
	}
	check := count == 0
	count, err = c.sadd(txn, args[1], args[2:], check)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(count)
}

// (sets) SCARD key
func (c *Command) SCardHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(SCARD_COMMAND)
	}
	ret, err := c.GetCountByKey(txn, args[0], SetType)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(ret)
}

// SUNIONSTORE destination key [key ...]
func (c *Command) SUnionStoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(SUNIONSTORE_COMMAND)
	}
	return c.sstore(txn, args, c.union)
}

// (sets) SINTERSTORE destination key [key ...]
func (c *Command) SInterStoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(SINTERSTORE_COMMAND)
	}
	return c.sstore(txn, args, c.inter)
}

// (sets) SDIFFSTORE destination key [key ...]
func (c *Command) SDiffStoreHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(SDIFFSTORE_COMMAND)
	}
	return c.sstore(txn, args, c.diff)
}

// (sets) SDIFF key [key ...]
func (c *Command) SDiffHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(SDIFF_COMMAND)
	}

	ret, err := c.diff(txn, args, SetType, OnlyKey, nil)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// SINTER key [key ...]
func (c *Command) SInterHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(SINTER_COMMAND)
	}

	ret, err := c.inter(txn, args, SetType, OnlyKey, nil)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func (c *Command) sstore(txn *store.Txn, args [][]byte, sfunc SFunc) interface{} {
	ret, err := sfunc(txn, args[1:], SetType, OnlyKey, nil)
	if err != nil {
		return txn.SetError(err)
	}

	create := len(ret) > 0
	object, err := c.DeleteThenCreateUUIDObject(txn, SetType, args[0], create)
	if err != nil {
		return txn.SetError(err)
	}
	if !create {
		return redcon.SimpleInt(0)
	}

	for _, k := range ret {
		svalue := &Value{Timestamp: txn.Timestamp}
		skey := object.GetValueBytes(k.([]byte))
		_, err = PutOrDeleteKV(txn, object, skey, EncodeValue(svalue), 1)
		if err != nil {
			return txn.SetError(err)
		}
	}
	return redcon.SimpleInt(len(ret))
}

// SUNION key [key ...]
func (c *Command) SUnionHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(SUNION_COMMAND)
	}
	ret, err := c.union(txn, args, SetType, OnlyKey, nil)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func (c *Command) inter(txn *store.Txn, args [][]byte, typo ObjectType, getType int, weight []float64) ([]interface{}, error) {
	_, ok := GetKeyFuncs[typo]
	if !ok {
		return nil, xerror.ErrNotSupport
	}

	glist := make(map[int]*Object)
	llist := make(map[int]*Object)
	var object *Object
	var mini int
	var err error
	for i := 0; i < len(args); i++ {
		o := NewObject(txn, typo, args[i])
		k := o.GetKeyBytes()
		err = getTxnObject(txn, k, o, false)
		if err == store.KeyNotFound {
			if i < len(args)-1 {
				err = c.checkValidObjectArgs(txn, args[i+1:], typo)
				if err != nil {
					return nil, err
				}
			}
			return EmptyInterface, nil
		} else if err == xerror.WrongTypeErr {
			if typo == ZsetType && o.Type == SetType {
			} else {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
		if object == nil {
			object = o
		} else if txn.Config.Redis.SetOperation == config.SETOP_SCAN {
			llist[i] = o
		} else {
			glist[i] = o
		}
	}
	utils.ZapLog.Debug("inter", zap.Int("glist", len(glist)), zap.Int("llist", len(llist)))
	getKeyFunc := GetKeyFuncs[object.Type]
	start := getKeyFunc(object, nil)
	end := utils.PrefixNext(start)
	ret := make([]interface{}, 0)
	var iterList *store.IterList
	var cbErr error
	err = txn.List(start, end, txn.Config.Redis.ScanMaxCount, func(key, value []byte) bool {
		if len(key) < len(start) {
			return true
		}
		// init
		k := key[len(start):]
		if iterList == nil && len(llist) > 0 {
			iterList = store.NewIterList()
			for idx, o := range llist {
				getKeyFunc := GetKeyFuncs[o.Type]
				p := getKeyFunc(o, nil)
				s := getKeyFunc(o, k)
				e := utils.PrefixNext(p)
				iter, err := txn.Iter(s, e, false, 0)
				if err != nil {
					cbErr = err
					return false
				}
				iterList.Add(p, idx, iter)
			}
		}

		// list check
		lvalues := make(map[int][]byte)
		if iterList != nil {
			lvalues, err = iterList.NextUntil(k, true)
			if err != nil {
				cbErr = err
				return false
			}
			if len(lvalues) < len(llist) {
				return true
			}
		}

		// get check
		for idx, o := range glist {
			getKeyFunc := GetKeyFuncs[o.Type]
			skey := getKeyFunc(o, k)
			v, err := txn.Get(skey)
			if err == store.KeyNotFound {
				return true
			} else if err != nil {
				cbErr = err
				return false
			}
			lvalues[idx] = v
		}

		if getType&OnlyKey == OnlyKey {
			ret = append(ret, k)
		}
		if getType&OnlyValue == OnlyValue && typo == ZsetType {
			zv := &Value{}
			DecodeValue(value, zv)
			score := utils.Number{}
			if len(zv.Value) > 0 {
				score.Decode(zv.Value)
			} else {
				score.Float64 = 1
			}
			utils.ZapLog.Debug("inter", zap.ByteString("key", k), zap.String("score", score.String()))
			if len(weight) > 0 {
				score.Float64 *= weight[mini]
			}
			for idx, v := range lvalues {
				zv := &Value{}
				DecodeValue(v, zv)
				s := utils.Number{}
				if len(zv.Value) > 0 {
					s.Decode(zv.Value)
				} else {
					s.Float64 = 1
				}
				utils.ZapLog.Debug("inter", zap.ByteString("key", k), zap.String("score", s.String()))
				if len(weight) > 0 {
					s.Float64 *= weight[idx]
				}
				if getType&SumAGG == SumAGG {
					score.Add(s)
				} else if getType&MaxAGG == MaxAGG {
					if s.Compare(score) > 0 {
						score = s
					}
				} else if getType&MinAGG == MinAGG {
					if s.Compare(score) < 0 {
						score = s
					}
				}
			}
			utils.ZapLog.Debug("inter", zap.ByteString("key", k), zap.String("score-sum", score.String()))
			ret = append(ret, score)
		}
		return true
	})
	if cbErr != nil {
		return nil, cbErr
	}
	if err != nil {
		return nil, err
	}
	if iterList != nil {
		iterList.Close()
	}
	return ret, nil
}

func (c *Command) union(txn *store.Txn, args [][]byte, typo ObjectType, getType int, weight []float64) ([]interface{}, error) {
	utils.ZapLog.Debug("union", zap.ByteStrings("args", args), zap.Int("weight", len(weight)), zap.Int("getType", getType))
	getKeyFunc, ok := GetKeyFuncs[typo]
	if !ok {
		return nil, xerror.ErrNotSupport
	}

	var err error
	iterList := store.NewIterList()
	for i := 0; i < len(args); i++ {
		var p []byte
		object := NewObject(txn, typo, args[i])
		k := object.GetKeyBytes()
		err = getTxnObject(txn, k, object, false)
		if err == store.KeyNotFound {
			continue
		} else if err == xerror.WrongTypeErr {
			if typo == ZsetType && object.Type == SetType {
				p = GetKeyFuncs[SetType](object, nil)
			} else {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		} else {
			p = getKeyFunc(object, nil)
		}
		s := p
		e := utils.PrefixNext(p)
		iter, err := txn.Iter(s, e, false, 0)
		if err != nil {
			return nil, err
		}
		iterList.Add(p, i, iter)
	}

	ret := make([]interface{}, 0)
	var preKey []byte
	var preScore utils.Number
	var i int
	for {
		kv, err := iterList.Next()
		if err != nil {
			return nil, err
		}
		i++
		new := preKey != nil && !bytes.Equal(preKey, kv.Key)
		if getType&OnlyKey == OnlyKey {
			if new {
				ret = append(ret, preKey)
			}
			utils.ZapLog.Debug("union", zap.ByteString("key", kv.Key), zap.Int("i", i), zap.ByteString("pre-key", preKey))
			preKey = kv.Key
		}
		if getType&OnlyValue == OnlyValue && typo == ZsetType {
			if new {
				utils.ZapLog.Debug("union", zap.Int("i", i), zap.String("ret-score", preScore.String()))
				ret = append(ret, preScore)
				preScore = utils.Number{}
			}
			if kv.Key == nil {
				break
			}
			zv := &Value{}
			DecodeValue(kv.Value, zv)
			score := utils.Number{}
			if len(zv.Value) > 0 {
				score.Decode(zv.Value)
			} else {
				score.Float64 = 1
			}
			if len(weight) > 0 {
				score.Float64 *= weight[kv.Idx]
			}
			if getType&SumAGG == SumAGG {
				preScore.Add(score)
			} else if getType&MaxAGG == MaxAGG {
				if i == 1 || new || score.Compare(preScore) > 0 {
					preScore = score
				}
			} else if getType&MinAGG == MinAGG {
				if i == 1 || new || score.Compare(preScore) < 0 {
					preScore = score
				}
			}
			utils.ZapLog.Debug("union", zap.Int("i", i), zap.ByteString("key", kv.Key),
				zap.String("score", score.String()), zap.String("pre-score", preScore.String()))
		}
		if kv.Key == nil {
			break
		}
	}
	return ret, nil
}

func (c *Command) checkValidObjectArgs(txn *store.Txn, args [][]byte, typo ObjectType) error {
	var err error
	for i := 0; i < len(args); i++ {
		o := NewObject(txn, typo, args[i])
		k := o.GetKeyBytes()
		utils.ZapLog.Debug("diff", zap.String("key", string(args[i])), zap.ByteString("k", k))
		err = getTxnObject(txn, k, o, false)
		if err != store.KeyNotFound && err != nil {
			return err
		}
	}
	return nil
}

func (c *Command) diff(txn *store.Txn, args [][]byte, typo ObjectType, getType int, weight []float64) ([]interface{}, error) {
	object := NewObject(txn, typo, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		err = c.checkValidObjectArgs(txn, args[1:], typo)
		if err != nil {
			return nil, err
		}
		return EmptyInterface, nil
	} else if err == xerror.WrongTypeErr {
		if typo == ZsetType && object.Type == SetType {
		} else {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	getKeyFunc, ok := GetKeyFuncs[object.Type]
	if !ok {
		return nil, xerror.ErrNotSupport
	}
	start := getKeyFunc(object, nil)
	end := utils.PrefixNext(start)
	ret := make([]interface{}, 0)
	if len(args) == 1 {
		err = txn.List(start, end, txn.Config.Redis.ScanMaxCount, func(key, value []byte) bool {
			if len(key) < len(start) {
				return true
			}
			ret = append(ret, key[len(start):])
			return true
		})
		if err != nil {
			return nil, err
		}
		return ret, nil
	}

	glist := make(map[int]*Object)
	llist := make(map[int]*Object)
	for i := 1; i < len(args); i++ {
		o := NewObject(txn, typo, args[i])
		k := o.GetKeyBytes()
		utils.ZapLog.Debug("diff", zap.String("key", string(args[i])), zap.ByteString("k", k))
		err = getTxnObject(txn, k, o, false)
		if err == store.KeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}

		if txn.Config.Redis.SetOperation == config.SETOP_GET {
			glist[i] = o
		} else {
			llist[i] = o
		}
	}

	utils.ZapLog.Debug("diff", zap.Int("glist", len(glist)), zap.Int("llist", len(llist)))

	var iterList *store.IterList
	var cbErr error
	err = txn.List(start, end, txn.Config.Redis.ScanMaxCount, func(key, value []byte) bool {
		if len(key) < len(start) {
			return true
		}
		// init
		k := key[len(start):]
		if iterList == nil && len(llist) > 0 {
			iterList = store.NewIterList()
			for i, o := range llist {
				getKeyFunc := GetKeyFuncs[o.Type]
				p := getKeyFunc(o, nil)
				s := getKeyFunc(o, k)
				e := utils.PrefixNext(p)
				iter, err := txn.Iter(s, e, false, 0)
				if err != nil {
					cbErr = err
					return false
				}
				iterList.Add(p, i, iter)
			}
		}
		// get check
		for _, o := range glist {
			getKeyFunc := GetKeyFuncs[o.Type]
			skey := getKeyFunc(o, k)
			_, err := txn.Get(skey)
			if err == nil {
				return true
			} else if err == store.KeyNotFound {
				continue
			} else {
				cbErr = err
				return false
			}
		}

		// list check
		if iterList != nil {
			values, err := iterList.NextUntil(k, false)
			if err != nil {
				cbErr = err
				return false
			}
			if len(values) > 0 {
				return true
			}
		}
		if getType&OnlyKey == OnlyKey {
			ret = append(ret, k)
		}
		if getType&OnlyValue == OnlyValue && typo == ZsetType {
			zv := &Value{}
			DecodeValue(value, zv)
			score := utils.Number{}
			if len(zv.Value) > 0 {
				score.Decode(zv.Value)
			} else {
				score.Float64 = 1
			}
			ret = append(ret, score)
		}
		return true
	})
	if cbErr != nil {
		return nil, cbErr
	}
	if err != nil {
		return nil, err
	}
	if iterList != nil {
		iterList.Close()
	}
	return ret, nil
}

// (sets) SPOP key [count]
func (c *Command) SPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(SPOP_COMMAND)
	}
	var count int
	var err error
	simple := true
	if len(args) == 2 {
		count, err = utils.GetPositiveInt(args[1])
		if err == utils.ErrInvalidInt && count == 0 {
			return EmptyBytes
		}
		if err != nil {
			return txn.SetError(xerror.ErrNotPositiveInteger)
		}
		simple = false
	} else {
		count = 1
	}
	object := NewObject(txn, SetType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return EmptyBytes
	}
	if err != nil {
		return txn.SetError(err)
	}
	start := object.GetValueBytes(nil)
	end := utils.PrefixNext(start)
	var cbErr error
	ret := make([][]byte, 0, count)
	err = txn.List(start, end, count, func(key, value []byte) bool {
		if len(key) < len(start) {
			return true
		}
		_, cbErr = PutOrDeleteKV(txn, object, key, nil, -1)
		if cbErr != nil {
			return false
		}
		ret = append(ret, key[len(start):])
		return true
	})
	if cbErr != nil {
		return txn.SetError(cbErr)
	}
	if err != nil && err != store.ReachLimit {
		return txn.SetError(err)
	}
	if simple && len(ret) > 0 {
		return ret[0]
	}
	return ret
}

// (sets) SRANDMEMBER key [count]
func (c *Command) SRandMemberHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(SRANDMEMBER_COMMAND)
	}
	single := len(args) == 1
	var count int
	ucount := 1
	var err error
	if len(args) == 2 {
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return txn.SetError(xerror.ErrNotInteger)
		}
		if count == 0 {
			return [][]byte{}
		} else if count > 0 {
			ucount = count
		} else {
			ucount = -count
		}
	}
	ret, err := c.smembers(txn, args, ucount)
	if err != nil {
		return txn.SetError(err)
	}
	if single {
		if len(ret) == 0 {
			return nil
		}
		return ret[0]
	}
	if count < 0 && len(ret) < ucount {
		rand.Seed(int64(txn.Timestamp))
		for i := len(ret); i < ucount; i++ {
			ret = append(ret, ret[rand.Intn(len(ret))])
		}
	}
	return ret
}

// (sets) SMEMBERS key
func (c *Command) SMembersHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(SMEMBERS_COMMAND)
	}
	ret, err := c.smembers(txn, args, txn.Config.Redis.ScanMaxCount)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func (c *Command) smembers(txn *store.Txn, args [][]byte, limit int) ([][]byte, error) {
	object := NewObject(txn, SetType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return EmptyBytes, nil
	}
	if err != nil {
		return nil, err
	}
	start := object.GetValueBytes(nil)
	end := utils.PrefixNext(start)
	var cbErr error
	ret := make([][]byte, 0)
	err = txn.List(start, end, limit, func(key, value []byte) bool {
		if len(key) < len(start) {
			return true
		}
		ret = append(ret, key[len(start):])
		return true
	})
	if cbErr != nil {
		return nil, cbErr
	}
	if err != nil && err != store.ReachLimit {
		return nil, err
	}
	return ret, nil
}

// (sets) SSCAN key cursor [MATCH pattern] [COUNT count]
func (c *Command) SScanHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(SSCAN_COMMAND)
	}
	return c.TypeScan(txn, SetType, args, OnlyKey)
}
