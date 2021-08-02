package command

import (
	"bytes"
	"encoding/base64"
	"strconv"
	"strings"

	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/utils/glob"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

func IsExpired(txn *store.Txn, o *Object) (int, bool) {
	if o.TTL == 0 {
		return 0, false
	}
	if o.TTL > txn.Now {
		return int((o.TTL - txn.Now) / 1000), false
	}
	return 0, true
}

// (generic) EXPIRE key seconds
func (c *Command) ExpireHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(EXPIRE_COMMAND)
	}

	expire, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}

	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return SimpleInt(0)
	} else if err != nil && err != xerror.WrongTypeError {
		return txn.SetError(err)
	}

	if expire <= 0 {
		err = DeleteKey(txn, key, object, txn.Now)
		if err != nil {
			return txn.SetError(err)
		}
		return SimpleInt(1)
	}
	newTTL := txn.Now + expire*1000
	if object.TTL == newTTL {
		return SimpleInt(1)
	}

	if object.TTL > 0 {
		ttlKey := object.GetTTLKeyBytes()
		err = txn.Del(ttlKey)
		if err != nil {
			return err
		}
	}

	object.TTL = newTTL
	object.Timestamp = txn.Timestamp
	ttlKey := object.GetTTLKeyBytes()
	err = txn.Put(ttlKey, []byte{1})
	if err != nil {
		return txn.SetError(err)
	}
	err = txn.Put(key, ObjectEncode(object))
	if err != nil {
		return txn.SetError(err)
	}
	return SimpleInt(1)
}

// (generic) EXISTS key
func (c *Command) ExistsHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(EXISTS_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return SimpleInt(0)
	} else if err != nil && err != xerror.WrongTypeError {
		return txn.SetError(err)
	}
	return SimpleInt(1)
}

// (generic) TTL key
func (c *Command) TTLHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(TTL_COMMAND)
	}
	object := NewObject(txn.UserId, txn.DBId, KeyType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return SimpleInt(-2)
	} else if err != nil && err != xerror.WrongTypeError {
		return txn.SetError(err)
	} else if object.TTL > 0 {
		expireSecond, isExpired := IsExpired(txn, object)
		if isExpired {
			return SimpleInt(-2)
		} else {
			return SimpleInt(expireSecond)
		}
	} else {
		return SimpleInt(-1)
	}
}

func DeleteKeyReturn(txn *store.Txn, key []byte, object *Object, now int64) interface{} {
	err := DeleteKey(txn, key, object, now)
	if err != nil {
		return txn.SetError(err)
	}
	return 1
}

func DeleteKey(txn *store.Txn, key []byte, object *Object, now int64) error {
	var err error
	if object.TTL > 0 {
		ttlKey := object.GetTTLKeyBytes()
		err = txn.Del(ttlKey)
		if err != nil {
			return err
		}
	}

	if !object.IsSimple() {
		object.TTL = txn.Now
		ttlValue := object.GetTTLValueBytes()
		err = txn.Put(ttlValue, []byte{1})
		if err != nil {
			return err
		}
	}

	err = txn.Del(key)
	if err != nil {
		return err
	}
	return nil
}

// (generic) DEL key [key ...]
func (c *Command) DELHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) == 0 {
		return txn.SetWrongArgs(DEL_COMMAND)
	}

	count := 0
	for i := range args {
		object := NewObject(txn.UserId, txn.DBId, KeyType, args[i])
		key := object.GetKeyBytes()
		err := getTxnObject(txn, key, object, true)
		if err == store.KeyNotFound {
			continue
		} else if err != nil && err != xerror.WrongTypeError {
			return txn.SetError(err)
		}
		count++
		err = DeleteKey(txn, key, object, txn.Now)
		if err != nil {
			return txn.SetError(err)
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

func (c *Command) getScanOptions(opts [][]byte) (*scanOptions, error) {
	scanOptions := &scanOptions{
		count:  10,
		match:  "*",
		typo:   KeyType,
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

			if count > c.cfg.Key.ScanMaxCount {
				count = c.cfg.Key.ScanMaxCount
			}
			scanOptions.count = count
		case "type":
			typo, ok := ObjectNameMap[utils.B2S(opts[i+1])]
			if !ok {
				scanOptions.typo = UnknownType
			} else {
				scanOptions.typo = typo
			}
		case "cursor":
			name := strings.ToLower(utils.B2S(opts[i+1]))
			cursor, ok := CursorMap[name]
			if !ok {
				return nil, xerror.ErrSyntax
			}
			scanOptions.cursor = cursor
		}
	}
	return scanOptions, nil
}

type ScanResult struct {
	Cursor int
	Keys   []string
}

// (generic) SCAN cursor [MATCH pattern] [COUNT count] [TYPE type] [CURSOR cursor]
func (c *Command) ScanHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(SCAN_COMMAND)
	}

	cursor := args[0]
	opts := args[1:]
	scanOpt, err := c.getScanOptions(opts)
	if err != nil {
		return txn.SetError(err)
	}

	if scanOpt.typo == UnknownType {
		return []interface{}{0, []string{}}
	}

	prefix := glob.Prefix(scanOpt.match)
	start := GetDataPrefix(txn.UserId, txn.DBId, KeyType, utils.S2B(prefix))
	prefixLen := len(start)
	end := utils.PrefixNext(start)
	if scanOpt.cursor == ServerCursor {
		cursorInt, err := strconv.ParseInt(utils.B2S(cursor), 10, 64)
		if err != nil {
			return txn.SetError(xerror.InvalidCursor)
		}
		if cursorInt < 0 {
			return txn.SetError(xerror.InvalidCursor)
		}

		if cursorInt > 0 {
			cur, ok := c.GetCursor(uint64(cursorInt))
			if ok {
				cur := utils.NextKey(cur)
				start = append(start, cur...)
			}
		}
	} else {
		if !bytes.Equal(cursor, []byte{'0'}) {
			cur, err := base64.StdEncoding.DecodeString(utils.B2S(cursor))
			if err != nil {
				return txn.SetError(xerror.InvalidCursor)
			}
			cur = utils.NextKey(cur)
			start = append(start, cur...)
		}
	}

	if _, err := glob.Match(scanOpt.match, utils.B2S(start)); err != nil {
		utils.ZapLog.Error("scan invalid match", zap.String("remote", txn.RemoteAddr()),
			zap.Uint64("timestamp", txn.Timestamp), zap.String("match", scanOpt.match), zap.ByteString("start", start))
		return txn.SetError(err)
	}

	retKeys := make([]string, 0)
	var lastKey []byte
	callback := func(key, value []byte) bool {
		lastKey = key
		object, err := GetObjectFromKV(key, value)
		if err != nil {
			utils.ZapLog.Error("scan object", zap.String("remote", txn.RemoteAddr()),
				zap.Uint64("timestamp", txn.Timestamp), zap.Binary("key", key), zap.Binary("value", value), zap.Error(err))
			return true
		}
		utils.ZapLog.Debug("scan object", zap.Any("object", object))
		matched, err := glob.Match(scanOpt.match, utils.B2S(object.Key))
		if err != nil {
			utils.ZapLog.Error("scan invalid match", zap.String("remote", txn.RemoteAddr()),
				zap.Uint64("timestamp", txn.Timestamp), zap.String("match", scanOpt.match), zap.ByteString("key", object.Key))
			return false
		}
		if !matched {
			return true
		}

		if object.Type != scanOpt.typo {
			return true
		}
		if object.TTL > 0 {
			_, isExpired := IsExpired(txn, object)
			if isExpired {
				return true
			}
		}
		retKeys = append(retKeys, string(object.Key))
		return true
	}
	utils.ZapLog.Debug("scan result", zap.String("remote", txn.RemoteAddr()),
		zap.Uint64("timestamp", txn.Timestamp), zap.Strings("result", retKeys), zap.ByteString("last", lastKey))
	err = txn.List(start, end, scanOpt.count, callback)
	if err == nil || len(lastKey) <= prefixLen {
		return []interface{}{0, retKeys}
	} else if err != store.ReachLimit {
		return txn.SetError(err)
	}

	cur := lastKey[prefixLen:]
	if scanOpt.cursor == ServerCursor {
		c.SetCursor(txn.Timestamp, cur)
		return []interface{}{txn.Timestamp, retKeys}
	} else {
		cur := base64.StdEncoding.EncodeToString(cur)
		return []interface{}{cur, retKeys}
	}
}
