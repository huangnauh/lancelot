package command

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/utils/glob"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const (
	HashKey   = 0x01
	HashValue = 0x02
	HashBoth  = HashKey | HashValue
)

//(hash) HEXISTS key field
func (c *Command) HExistsHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(HEXISTS_COMMAND)
	}
	value, err := c.hget(txn, args)
	if err != nil {
		return txn.SetError(err)
	}
	if value == nil {
		return redcon.SimpleInt(0)
	}
	return redcon.SimpleInt(1)
}

type HashKV struct {
	Key   []byte
	Value []byte
}

//(hash) HGETALL key
func (c *Command) HGetAllHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(HGETALL_COMMAND)
	}
	ret, err := c.hgetall(txn, args, HashBoth, 0)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// (hash) HRANDFIELD key [count [WITHVALUES]]
func (c *Command) HRandFieldHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 || len(args) > 3 {
		return txn.SetWrongArgs(HRANDFIELD_COMMAND)
	}
	single := len(args) == 1
	var count int
	var ucount int
	if len(args) > 1 {
		count, err := strconv.Atoi(string(args[1]))
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
	getType := HashKey
	if len(args) > 2 {
		if strings.ToLower(utils.B2S(args[2])) != "withvalues" {
			return txn.SetError(xerror.ErrSyntax)
		}
		getType = HashBoth
	}
	ret, err := c.hgetall(txn, args, getType, ucount)
	if err != nil {
		return txn.SetError(err)
	}

	if single {
		if len(ret) == 0 {
			return nil
		}
		return ret[0]
	}

	rand.Seed(int64(txn.Timestamp))
	if count < 0 && len(ret) < ucount {
		for i := len(ret); i < ucount; i++ {
			ret = append(ret, ret[rand.Intn(len(ret))])
		}
	}
	return ret
}

//(hash) HKEYS key
func (c *Command) HKeysHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(HKEYS_COMMAND)
	}
	ret, err := c.hgetall(txn, args, HashKey, 0)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(hash) HVALS key
func (c *Command) HValsHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(HVALS_COMMAND)
	}
	ret, err := c.hgetall(txn, args, HashValue, 0)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(hash) HLEN key
func (c *Command) HLenHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(HLEN_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	counts, err := c.ListCount(txn, txn.UserId, txn.DBId, HashType, uint16(object.Hash), object.Value)
	if err != nil {
		return txn.SetError(err)
	}
	var hlen int64
	for _, count := range counts {
		hlen += count.Value
	}
	return redcon.SimpleInt(hlen)
}

//(hash) HSCAN key cursor [MATCH pattern] [COUNT count]
func (c *Command) HScanHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(HSCAN_COMMAND)
	}
	cursor := args[1]
	opts := args[2:]
	scanOpt, err := c.getScanOptions(opts)
	if err != nil {
		return txn.SetError(err)
	}
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return EmptyCursor
	} else if err != nil {
		return txn.SetError(err)
	}
	cursorPrefix := glob.Prefix(scanOpt.match)
	prefix := object.GetKeyFieldBytes(nil)
	start := object.GetKeyFieldBytes(utils.S2B(cursorPrefix))
	end := utils.PrefixNext(prefix)
	start, err = c.checkCursor(scanOpt, cursor, HashCursor, start)
	if err != nil {
		return txn.SetError(err)
	}
	ret := make([][]byte, 0)
	var lastKey []byte
	var callbackErr error
	err = txn.List(start, end, scanOpt.count, func(key, value []byte) bool {
		lastKey = key
		if len(key) < len(start) {
			return true
		}
		hkey := key[len(start):]
		var matched bool
		matched, callbackErr = glob.Match(scanOpt.match, utils.B2S(hkey))
		if err != nil {
			utils.ZapLog.Error("hscan invalid match", zap.String("remote", txn.RemoteAddr()),
				zap.Uint64("timestamp", txn.Timestamp), zap.String("match", scanOpt.match), zap.ByteString("key", hkey))
			return false
		}
		if !matched {
			return true
		}
		hvalue := &Value{}
		_ = DecodeValue(value, hvalue)
		ret = append(ret, hkey, hvalue.Value)
		return true
	})
	if callbackErr != nil {
		return txn.SetError(callbackErr)
	}
	if err == nil || len(lastKey) <= len(prefix) {
		return []interface{}{0, ret}
	} else if err != store.ReachLimit {
		return txn.SetError(err)
	}
	cur := lastKey[len(prefix):]
	if scanOpt.cursor == ServerCursor {
		c.SetCursor(fmt.Sprintf("%s%d", HashCursor, txn.Timestamp), cur)
		return []interface{}{txn.Timestamp, ret}
	} else {
		cur := base64.StdEncoding.EncodeToString(cur)
		return []interface{}{cur, ret}
	}
}

func (c *Command) hgetall(txn *store.Txn, args [][]byte, getType int, limit int) ([][]byte, error) {
	ret := make([][]byte, 0)
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return ret, nil
	} else if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > c.cfg.Key.ScanMaxCount {
		limit = c.cfg.Key.ScanMaxCount
	}
	start := object.GetKeyFieldBytes(nil)
	end := utils.PrefixNext(start)
	err = txn.List(start, end, limit, func(key []byte, value []byte) bool {
		if len(key) < len(start) {
			return true
		}

		if getType&HashKey == HashKey {
			ret = append(ret, key[len(start):])
		}
		if getType&HashValue == HashValue {
			hvalue := &Value{}
			_ = DecodeValue(value, hvalue)
			ret = append(ret, hvalue.Value)
		}
		return true
	})
	if err == nil || err == store.ReachLimit {
		return ret, nil
	}
	return nil, err
}

//(hash) HMGET key field [field ...]
func (c *Command) HMGetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(HMGET_COMMAND)
	}
	ret := make([]interface{}, len(args)-1)
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return ret
	} else if err != nil {
		return txn.SetError(err)
	}
	for i := 1; i < len(args); i++ {
		hkey := object.GetKeyFieldBytes(args[i])
		value, err := txn.Get(hkey)
		if err == store.KeyNotFound {
			continue
		} else if err != nil {
			return txn.SetError(err)
		}
		hvalue := &Value{}
		err = DecodeValue(value, hvalue)
		if err != nil {
			continue
		}
		ret[i-1] = hvalue.Value
	}
	return ret
}

//(hash) HSTRLEN key field
func (c *Command) HStrLenHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(HSTRLEN_COMMAND)
	}
	value, err := c.hget(txn, args)
	if err != nil {
		return txn.SetError(err)
	}
	return len(value)
}

//(hash) HGET key field
func (c *Command) HGetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(HGET_COMMAND)
	}
	ret, err := c.hget(txn, args)
	if err != nil {
		return txn.SetError(err)
	}
	if ret == nil {
		return nil
	}
	return ret
}

func (c *Command) hget(txn *store.Txn, args [][]byte) ([]byte, error) {
	field := args[1]
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	hkey := object.GetKeyFieldBytes(field)
	value, err := txn.Get(hkey)
	if err == store.KeyNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	hvalue := &Value{}
	err = DecodeValue(value, hvalue)
	if err != nil {
		return nil, nil
	}
	return hvalue.Value, nil
}

//(hash) HDEL key field [field ...]
func (c *Command) HDelHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(HDEL_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, HashType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}

	var ret int64
	for start := 1; start < len(args); start++ {
		hkey := object.GetKeyFieldBytes(args[start])
		_, err = txn.Get(hkey)
		if err == store.KeyNotFound {
			continue
		} else if err != nil {
			return txn.SetError(err)
		}

		ret++
		_, err = c.hsetKV(txn, object, hkey, nil, -1)
		if err != nil {
			return txn.SetError(err)
		}
	}
	return SimpleInt(ret)
}

//(hash) HINCRBYFLOAT key field increment
func (c *Command) HIncrByFloatHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(HINCRBYFLOAT_COMMAND)
	}
	increment, err := strconv.ParseFloat(utils.B2S(args[2]), 64)
	if err != nil {
		return txn.SetError(xerror.ErrInvalidFloat)
	}
	object, err := c.GetOrCreateUUIDObject(txn, HashType, args[0])
	if err != nil {
		return txn.SetError(err)
	}
	hkey := object.GetKeyFieldBytes(args[1])
	value, err := txn.Get(hkey)
	var floatValue float64
	var delta int64
	if err == store.KeyNotFound {
		delta = 1
	} else if err != nil {
		return txn.SetError(err)
	} else {
		hvalue := &Value{}
		err = DecodeValue(value, hvalue)
		if err == nil {
			floatValue, err = strconv.ParseFloat(utils.B2S(hvalue.Value), 64)
			if err != nil {
				return txn.SetError(xerror.ErrInvalidFloat)
			}
			if increment == 0 {
				return floatValue
			}
		}
	}
	floatValue += increment
	hvalue := &Value{
		Value:     utils.S2B(strconv.FormatFloat(floatValue, 'f', -1, 64)),
		Timestamp: txn.Timestamp,
	}
	_, err = c.hsetKV(txn, object, hkey, EncodeValue(hvalue), delta)
	if err != nil {
		return txn.SetError(err)
	}
	return floatValue
}

//(hash) HINCRBY key field increment
func (c *Command) HIncrByHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(HINCRBY_COMMAND)
	}
	increment, err := strconv.ParseInt(utils.B2S(args[2]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	object, err := c.GetOrCreateUUIDObject(txn, HashType, args[0])
	if err != nil {
		return txn.SetError(err)
	}
	hkey := object.GetKeyFieldBytes(args[1])
	value, err := txn.Get(hkey)
	var delta int64
	var intValue int64
	if err == store.KeyNotFound {
		delta = 1
	} else if err != nil {
		return txn.SetError(err)
	} else {
		hvalue := &Value{}
		err = DecodeValue(value, hvalue)
		if err == nil {
			intValue, err = strconv.ParseInt(utils.B2S(hvalue.Value), 10, 64)
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
			if increment == 0 {
				return redcon.SimpleInt(intValue)
			}
		}
	}
	intValue += increment
	hvalue := &Value{Value: utils.S2B(strconv.FormatInt(intValue, 10)), Timestamp: txn.Timestamp}
	_, err = c.hsetKV(txn, object, hkey, EncodeValue(hvalue), delta)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(intValue)
}

func (c *Command) GetOrCreateUUIDObject(txn *store.Txn, typo ObjectType, arg []byte) (*Object, error) {
	object := NewObject(txn.UserId, txn.DBId, typo, arg)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		id, err := uuid.NewUUID()
		if err != nil {
			return object, err
		}
		object.Value = id[:]
	} else if err != nil {
		return object, err
	}
	object.Timestamp = txn.Timestamp

	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return object, err
	}
	return object, nil
}

//(hash) HMSET key field value [field value ...]
func (c *Command) HMSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 || len(args)%2 != 1 {
		return txn.SetWrongArgs(HMSET_COMMAND)
	}
	_, err := c.hset(txn, args, false)
	if err != nil {
		return txn.SetError(err)
	}
	return OK
}

//(hash) HSETNX key field value
func (c *Command) HSetNXHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(HSETNX_COMMAND)
	}
	ret, err := c.hset(txn, args, true)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(ret)
}

//(hash) HSET key field value [field value ...]
func (c *Command) HSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 || len(args)%2 != 1 {
		return txn.SetWrongArgs(HSET_COMMAND)
	}
	ret, err := c.hset(txn, args, false)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(ret)
}

func (c *Command) hset(txn *store.Txn, args [][]byte, checkExist bool) (int64, error) {
	var ret int64
	object, err := c.GetOrCreateUUIDObject(txn, HashType, args[0])
	if err != nil {
		return ret, err
	}

	err = txn.Put(object.Key, ObjectEncode(object))
	if err != nil {
		return ret, err
	}

	for start := 1; start < len(args); start += 2 {
		var delta int64
		hkey := object.GetKeyFieldBytes(args[start])
		_, err = txn.Get(hkey)
		if err == store.KeyNotFound {
			ret++
			delta = 1
		} else if err != nil {
			return ret, err
		} else if checkExist {
			continue
		}
		hvalue := &Value{Value: args[start+1], Timestamp: txn.Timestamp}
		_, err = c.hsetKV(txn, object, hkey, EncodeValue(hvalue), delta)
		if err != nil {
			return ret, err
		}
	}

	return ret, nil
}

func (c *Command) hsetKV(txn *store.Txn, object *Object, k, v []byte, delta int64) (int64, error) {
	var err error
	if v == nil {
		err = txn.Del(k)
	} else {
		err = txn.Put(k, v)
	}
	if err != nil {
		return 0, err
	}
	if delta == 0 {
		return 0, nil
	}

	count, err := c.GetCount(txn, txn.UserId, txn.DBId, HashType, uint64(object.Hash), object.Value, v)
	if err == store.KeyNotFound {
	} else if err != nil {
		return 0, err
	}
	count.Value += delta
	err = c.SetCount(txn, count)
	return count.Value, err
}
