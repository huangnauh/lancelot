package command

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"

	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/utils/glob"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

func IsExpired(txn *store.Txn, o *Object) (int64, bool) {
	if o.TTL == 0 {
		return 0, false
	}
	if o.TTL > txn.Now {
		return (o.TTL - txn.Now) / 1000, false
	}
	return 0, true
}

// PERSIST key
func (c *Command) PersistHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(PERSIST_COMMAND)
	}
	return c.expire(txn, args, 0, true)
}

// EXPIRETIME key
func (c *Command) ExpireTimeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(EXPIRETIME_COMMAND)
	}
	object := c.NewObject(txn, UnknownType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return SimpleInt(-2)
	} else if err != nil {
		return txn.SetError(err)
	}
	if object.TTL == 0 {
		return SimpleInt(-1)
	}
	return SimpleInt(object.TTL / 1000)
}

// PEXPIRETIME key
func (c *Command) PExpireTimeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(PEXPIRETIME_COMMAND)
	}
	object := c.NewObject(txn, UnknownType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return SimpleInt(-2)
	} else if err != nil {
		return txn.SetError(err)
	}
	if object.TTL == 0 {
		return SimpleInt(-1)
	}
	return SimpleInt(object.TTL)
}

// (generic) EXPIREAT key timestamp [NX|XX|GT|LT]
func (c *Command) ExpireAtHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(EXPIREAT_COMMAND)
	}
	timestamp, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	if math.MaxInt64/1000 <= timestamp || math.MinInt64/1000 >= timestamp {
		return txn.SetError(xerror.InvalidExpireError(EXPIREAT_COMMAND))
	}
	newTTL := timestamp * 1000
	return c.expire(txn, args, newTTL, false)
}

// PEXPIREAT key milliseconds-timestamp [NX|XX|GT|LT]
func (c *Command) PExpireAtHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(PEXPIREAT_COMMAND)
	}
	timestamp, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	newTTL := timestamp
	return c.expire(txn, args, newTTL, false)
}

// PEXPIRE key milliseconds [NX|XX|GT|LT]
func (c *Command) PExpireHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(PEXPIRE_COMMAND)
	}
	milliseconds, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	if math.MaxInt64-txn.Now <= milliseconds {
		return txn.SetError(xerror.InvalidExpireError(PEXPIRE_COMMAND))
	}
	newTTL := txn.Now + milliseconds
	return c.expire(txn, args, newTTL, false)
}

// (generic) EXPIRE key seconds [NX|XX|GT|LT]
func (c *Command) ExpireHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(EXPIRE_COMMAND)
	}

	expire, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}

	if math.MaxInt64/1000 <= expire || math.MinInt64/1000 >= expire {
		return txn.SetError(xerror.InvalidExpireError(EXPIRE_COMMAND))
	}

	newTTL := txn.Now + expire*1000
	return c.expire(txn, args, newTTL, false)
}

func (c *Command) expire(txn *store.Txn, args [][]byte, newTTL int64, clearTTL bool) interface{} {
	var err error
	var i int
	opt := &checkOption{}
	if len(args) > 2 {
		opt, i, err = getCheckOption(args[2:])
		if err != nil {
			return txn.SetError(err)
		}
		if len(args[2:]) > i {
			return txn.SetError(xerror.UnsupportedOptionError(args[2+i]))
		}
	}

	object := c.NewObject(txn, UnknownType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}

	if clearTTL {
		if object.TTL == 0 {
			return SimpleInt(0)
		}
		// clean ttl key
		ttlKey := object.GetTTLKeyBytes()
		err = txn.Del(ttlKey)
		if err != nil {
			return txn.SetError(err)
		}
		object.TTL = 0
		object.Timestamp = txn.Timestamp
		err = setTxnObject(txn, key, object, 0)
		if err != nil {
			return txn.SetError(err)
		}
		return SimpleInt(1)
	}

	if newTTL <= txn.Now {
		err = DeleteKey(txn, key, object, txn.Now, MinusCount)
		if err != nil {
			return txn.SetError(err)
		}
		return SimpleInt(1)
	}

	if object.TTL > 0 {
		if opt.Check&CheckNotExist == CheckNotExist {
			return SimpleInt(0)
		}

		if newTTL >= object.TTL && opt.Check&CheckLT == CheckLT {
			return SimpleInt(0)
		}

		if newTTL <= object.TTL && opt.Check&CheckGT == CheckGT {
			return SimpleInt(0)
		}

		if object.TTL == newTTL {
			return SimpleInt(1)
		}

		ttlKey := object.GetTTLKeyBytes()
		err = txn.Del(ttlKey)
		if err != nil {
			return err
		}
	} else {
		if opt.Check&CheckExist == CheckExist {
			return SimpleInt(0)
		}
		// A non-volatile key is treated as an infinite TTL
		if opt.Check&CheckGT == CheckGT {
			return SimpleInt(0)
		}
	}

	object.TTL = newTTL
	object.Timestamp = txn.Timestamp
	ttlKey := object.GetTTLKeyBytes()
	if object.IsSimple() {
		err = txn.Put(ttlKey, []byte{1})
	} else {
		err = txn.Put(ttlKey, EncodeTTLValue(object.Type, object.Value))
	}
	if err != nil {
		return txn.SetError(err)
	}
	err = setTxnObject(txn, key, object, 0)
	if err != nil {
		return txn.SetError(err)
	}
	return SimpleInt(1)
}

// (generic) EXISTS key [key ...]
func (c *Command) ExistsHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(EXISTS_COMMAND)
	}

	count := 0
	for _, arg := range args {
		object := c.NewObject(txn, UnknownType, arg)
		key := object.GetKeyBytes()
		err := getTxnObject(txn, key, object, false)
		if err == store.KeyNotFound {
			continue
		} else if err != nil {
			return txn.SetError(err)
		}
		count++
	}
	return redcon.SimpleInt(count)
}

func (c *Command) ttl(txn *store.Txn, args [][]byte) (int64, error) {
	object := c.NewObject(txn, UnknownType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return -2, nil
	} else if err != nil {
		return 0, err
	}
	if object.TTL == 0 {
		return -1, nil
	}
	return object.TTL, nil
}

// (generic) TTL key
func (c *Command) TTLHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(TTL_COMMAND)
	}
	ttl, err := c.ttl(txn, args)
	if err != nil {
		return txn.SetError(err)
	}
	if ttl < 0 {
		return redcon.SimpleInt(ttl)
	}
	val := ttl - txn.Now
	if val%1000 < 500 {
		return redcon.SimpleInt((ttl - txn.Now) / 1000)
	} else {
		return redcon.SimpleInt((ttl-txn.Now)/1000 + 1)
	}
}

// (generic) PTTL key
func (c *Command) PTTLHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(PTTL_COMMAND)
	}
	ttl, err := c.ttl(txn, args)
	if err != nil {
		return txn.SetError(err)
	}
	if ttl < 0 {
		return SimpleInt(ttl)
	}
	return redcon.SimpleInt(ttl - txn.Now)
}

func DeleteKeyReturn(txn *store.Txn, key []byte, object *Object, now int64, change ChangeType) interface{} {
	err := DeleteKey(txn, key, object, now, change)
	if err != nil {
		return txn.SetError(err)
	}
	return 1
}

func DeleteKey(txn *store.Txn, key []byte, object *Object, now int64, change ChangeType) error {
	return CleanKey(txn, key, object.GetTTLKeyBytes(), object, now, change)
}

func CleanKey(txn *store.Txn, key, ttlKey []byte, object *Object, valueTTL int64, change ChangeType) error {
	var err error
	if len(key) > 0 {
		err = setTxnObject(txn, key, object, change|DeleteKeyType)
		if err != nil {
			return err
		}
	}

	if len(ttlKey) > 0 {
		err = txn.Del(ttlKey)
		if err != nil {
			return err
		}
	}

	if !object.IsSimple() && len(object.Value) > 0 {
		if valueTTL > 0 {
			object.TTL = valueTTL
			ttlValue := object.GetTTLValueBytes()
			err = txn.Put(ttlValue, []byte{1})
			if err != nil {
				return err
			}
		} else if object.IsCountable() {
			// clean count
			err = DeleteCount(txn, object.UserId, object.Db, KeyPrefix, object.Value, txn.NowTime())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// (generic) TOUCH key [key ...]
func (c *Command) TouchHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) == 0 {
		return txn.SetWrongArgs(TOUCH_COMMAND)
	}
	return c.touchORDelete(txn, args, false)
}

// (generic) DEL key [key ...]
func (c *Command) DELHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) == 0 {
		return txn.SetWrongArgs(DEL_COMMAND)
	}
	return c.touchORDelete(txn, args, true)
}

// (generic) RENAMENX key newkey
func (c *Command) RenameNXHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(RENAMENX_COMMAND)
	}
	ret, err := c.rename(txn, args, true)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleInt(ret)
}

// (generic) RENAME key newkey
func (c *Command) RenameHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(RENAME_COMMAND)
	}
	_, err := c.rename(txn, args, false)
	if err != nil {
		return txn.SetError(err)
	}
	return OK
}

func (c *Command) rename(txn *store.Txn, args [][]byte, checkExist bool) (int, error) {
	fromObject := c.NewObject(txn, UnknownType, args[0])
	fromKey := fromObject.GetKeyBytes()
	err := getTxnObject(txn, fromKey, fromObject, false)
	if err == store.KeyNotFound {
		return 0, xerror.ErrNoSuchKey
	}
	if err != nil {
		return 0, err
	}

	if bytes.Equal(args[0], args[1]) {
		return 0, nil
	}

	toObject := c.NewObject(txn, UnknownType, args[1])
	toKey := toObject.GetKeyBytes()
	err = getTxnObject(txn, toKey, toObject, false)
	if err == store.KeyNotFound {
	} else if err != nil {
		return 0, err
	} else {
		if checkExist {
			return 0, nil
		}
		if fromObject.Type != toObject.Type {
			return 0, xerror.WrongTypeErr
		}
		err = setTxnObject(txn, toKey, toObject, DeleteKeyType|MinusCount)
		if err != nil {
			return 0, err
		}
	}

	err = setTxnObject(txn, fromKey, fromObject, DeleteKeyType)
	if err != nil {
		return 0, err
	}

	if fromObject.TTL > 0 {
		err = txn.Del(fromObject.GetTTLKeyBytes())
		if err != nil {
			return 0, err
		}
		fromObject.Key = toObject.Key
		err = txn.Put(fromObject.GetTTLKeyBytes(), []byte{1})
		if err != nil {
			return 0, err
		}
	}
	err = setTxnObject(txn, toKey, fromObject, 0)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// (generic) UNLINK key [key ...]
func (c *Command) UnlinkHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) == 0 {
		return txn.SetWrongArgs(UNLINK_COMMAND)
	}
	return c.touchORDelete(txn, args, true)
}

func (c *Command) touchORDelete(txn *store.Txn, args [][]byte, delete bool) interface{} {
	var count int64
	for i := range args {
		object := c.NewObject(txn, UnknownType, args[i])
		key := object.GetKeyBytes()
		err := getTxnObject(txn, key, object, false)
		if err == store.KeyNotFound {
			continue
		} else if err != nil {
			return txn.SetError(err)
		}
		count++
		if delete {
			err = DeleteKey(txn, key, object, txn.Now, MinusCount)
			if err != nil {
				return txn.SetError(err)
			}
		}
	}
	return SimpleInt(count)
}

type CursorType byte

const (
	ClusterCursor CursorType = 1
	ServerCursor  CursorType = 2
)

var CursorMap = map[string]CursorType{
	"cluster": ClusterCursor,
	"server":  ServerCursor,
}

type scanOptions struct {
	count  int
	match  string
	typo   ObjectType
	cursor CursorType
}

func (c *Command) getObjectType(txn *store.Txn, typo ObjectType) ObjectType {
	switch typo {
	case LListType:
		if txn.Config.Redis.ListType == config.BLIST {
			return BListType
		} else {
			return AListType
		}
	default:
		return typo
	}
}

func (c *Command) getType(txn *store.Txn, typo string) ObjectType {
	typo = strings.ToLower(typo)
	switch typo {
	case "string":
		return StringType
	case "json":
		return JsonType
	case "hash":
		return HashType
	case "list":
		if txn.Config.Redis.ListType == config.BLIST {
			return BListType
		} else {
			return AListType
		}
	case "set":
		return SetType
	case "zset":
		return ZsetType
	case "stream":
		return StreamType
	default:
		return UnknownType
	}
}

func (c *Command) getScanOptions(txn *store.Txn, opts [][]byte) (*scanOptions, error) {
	scanOptions := &scanOptions{
		count:  10,
		match:  "*",
		typo:   GeneralType,
		cursor: ServerCursor,
	}

	for i := 0; i < len(opts); i += 2 {
		if len(opts) < i+2 {
			return nil, xerror.WrongArgsError(SCAN_COMMAND)
		}

		switch strings.ToLower(utils.B2S(opts[i])) {
		case "match":
			scanOptions.match = utils.B2S(opts[i+1])
		case "count":
			count, err := strconv.Atoi(utils.B2S(opts[i+1]))
			if err != nil {
				return nil, xerror.ErrSyntax
			}

			if count <= 0 {
				return nil, xerror.ErrSyntax
			}

			if count > txn.Config.Redis.ScanMaxCount {
				count = txn.Config.Redis.ScanMaxCount
			}
			scanOptions.count = count
		case "type":
			scanOptions.typo = c.getType(txn, utils.B2S(opts[i+1]))
		case "cursor":
			name := strings.ToLower(utils.B2S(opts[i+1]))
			cursor, ok := CursorMap[name]
			if !ok {
				return nil, xerror.ErrSyntax
			}
			scanOptions.cursor = cursor
		default:
			return nil, xerror.ErrSyntax
		}
	}
	return scanOptions, nil
}

type ScanResult struct {
	Cursor int
	Keys   []string
}

func (c *Command) checkCursor(scanOpt *scanOptions, cursor []byte,
	cursorPrefix string, start []byte) ([]byte, error) {
	if scanOpt.cursor == ServerCursor {
		cursorInt, err := strconv.ParseInt(utils.B2S(cursor), 10, 64)
		if err != nil {
			return nil, xerror.InvalidCursor
		}
		if cursorInt < 0 {
			return nil, xerror.InvalidCursor
		}

		if cursorInt > 0 {
			cur, ok := c.GetCursor(fmt.Sprintf("%s:%d", cursorPrefix, cursorInt))
			if ok {
				cur := utils.NextKey(cur)
				start = append(start, cur...)
			} else {
				return nil, xerror.NotFoundCursor
			}
		}
	} else {
		if !bytes.Equal(cursor, []byte{'0'}) {
			cur, err := base64.StdEncoding.DecodeString(utils.B2S(cursor))
			if err != nil {
				return nil, xerror.InvalidCursor
			}
			cur = utils.NextKey(cur)
			start = append(start, cur...)
		}
	}
	if _, err := glob.Match(scanOpt.match, utils.B2S(start)); err != nil {
		return nil, err
	}
	return start, nil
}

// (generic) KEYS pattern
func (c *Command) KeysHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(KEYS_COMMAND)
	}
	match := utils.B2S(args[0])
	prefix := glob.Prefix(match)
	start := GetKeyBytes(DataPrefix, txn.UserId, txn.DBId, KeyPrefix, utils.S2B(prefix))
	end := utils.PrefixNext(start)
	retKeys, _, err := c.scan(txn, start, end, &scanOptions{
		match:  match,
		count:  txn.Config.Redis.ScanMaxCount,
		typo:   GeneralType,
		cursor: ServerCursor,
	})
	if err != nil {
		return txn.SetError(err)
	}
	return retKeys
}

// (generic) TYPE key
func (c *Command) TypeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(TYPE_COMMAND)
	}
	object := c.NewObject(txn, UnknownType, args[0])
	err := getTxnObject(txn, object.GetKeyBytes(), object, false)
	if err != nil {
		return txn.SetError(err)
	}
	return redcon.SimpleString(object.Type.Type())
}

// (generic) SCAN cursor [MATCH pattern] [COUNT count] [TYPE type] [CURSOR cursor]
func (c *Command) ScanHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(SCAN_COMMAND)
	}

	cursor := args[0]
	opts := args[1:]
	scanOpt, err := c.getScanOptions(txn, opts)
	if err != nil {
		return txn.SetError(err)
	}

	if scanOpt.typo == UnknownType {
		return EmptyCursor
	}

	prefix := glob.Prefix(scanOpt.match)
	start := GetKeyBytes(DataPrefix, txn.UserId, txn.DBId, KeyPrefix, utils.S2B(prefix))
	prefixLen := len(start)
	end := utils.PrefixNext(start)
	start, err = c.checkCursor(scanOpt, cursor, fmt.Sprintf("%s:%s:%s", string(GeneralType),
		string(scanOpt.typo), scanOpt.match), start)
	if err == xerror.NotFoundCursor {
		return []interface{}{0, EmptySlice}
	} else if err != nil {
		return txn.SetError(err)
	}

	retKeys, lastKey, err := c.scan(txn, start, end, scanOpt)
	if err != nil {
		return txn.SetError(err)
	}
	if len(lastKey) <= prefixLen {
		return []interface{}{0, retKeys}
	}

	cur := lastKey[prefixLen:]
	if scanOpt.cursor == ServerCursor {
		o := c.SetCursorByTimestamp(GeneralType, string(scanOpt.typo), scanOpt.match, txn.Timestamp, cur)
		return []interface{}{o, retKeys}
	} else {
		cur := base64.StdEncoding.EncodeToString(cur)
		return []interface{}{cur, retKeys}
	}
}

func (c *Command) scan(txn *store.Txn, start, end []byte, scanOpt *scanOptions) ([][]byte, []byte, error) {
	utils.ZapLog.Debug("scan", zap.ByteString("start", start), zap.ByteString("end", end),
		zap.Int("count", scanOpt.count), zap.String("match", scanOpt.match), zap.String("type", string(scanOpt.typo)))
	retKeys := make([][]byte, 0)
	var lastKey []byte
	var callbackErr error
	callback := func(key, value []byte) bool {
		lastKey = key
		object, err := GetObjectFromKV(key, value)
		if err != nil {
			utils.ZapLog.Error("scan object", zap.String("remote", txn.RemoteAddr()),
				zap.Uint64("timestamp", txn.Timestamp), zap.Binary("key", key), zap.Binary("value", value), zap.Error(err))
			return true
		}
		utils.ZapLog.Debug("scan object", zap.Any("keys", retKeys))
		var matched bool
		matched, callbackErr = glob.Match(scanOpt.match, utils.B2S(object.Key))
		if callbackErr != nil {
			utils.ZapLog.Error("scan invalid match", zap.String("remote", txn.RemoteAddr()),
				zap.Uint64("timestamp", txn.Timestamp), zap.String("match", scanOpt.match), zap.ByteString("key", object.Key))
			return false
		}
		if !matched {
			return true
		}

		if scanOpt.typo != GeneralType && object.Type != scanOpt.typo {
			return true
		}
		if object.TTL > 0 {
			_, isExpired := IsExpired(txn, object)
			if isExpired {
				return true
			}
		}
		retKeys = append(retKeys, object.Key)
		return len(retKeys) < scanOpt.count
	}
	utils.ZapLog.Debug("scan result", zap.String("remote", txn.RemoteAddr()),
		zap.Uint64("timestamp", txn.Timestamp), zap.ByteStrings("result", retKeys), zap.ByteString("last", lastKey))
	err := txn.List(start, end, txn.Config.Redis.ScanMaxCount, callback)
	if callbackErr != nil {
		return nil, lastKey, callbackErr
	}
	if err != nil && err != store.ReachLimit {
		return nil, lastKey, err
	}

	if len(retKeys) < scanOpt.count && err == nil {
		lastKey = nil
	}
	return retKeys, lastKey, nil
}

// all [start]
func (c *Command) AllHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) > 1 {
		return txn.SetWrongArgs(ALL_COMMAND)
	}
	retKeys := make([][][]byte, 0)
	callback := func(key, value []byte) bool {
		retKeys = append(retKeys, [][]byte{key, value})
		return true
	}
	start := []byte{0x00}
	end := []byte{0xff}
	if len(args) == 1 {
		start = args[0]
	}
	err := txn.List(start, end, 10000, callback)
	if err != nil {
		return txn.SetError(err)
	}
	return retKeys
}
