package command

import (
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/utils/glob"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const (
	DefaultTTL       = 30 * 60 * 1000 // 30 minutes
	PubSubDB   uint8 = 200

	NoOffset                 int64 = -1
	ALLPartition             int64 = -1
	DefaultLIMITPerPartition       = 100
	MaxLimitPerPartition           = 1000
	PUBSUB_HELP                    = "PUBSUB HELP"
)

func encodeMessageKeyPrefix(hash uint16) []byte {
	k := make([]byte, 2)
	binary.BigEndian.PutUint16(k, hash)
	return k
}

func encodeMessageKeyOffset(partition uint16, offset int64) []byte {
	k := make([]byte, 2+8)
	binary.BigEndian.PutUint16(k, partition)
	binary.BigEndian.PutUint64(k[2:], uint64(offset))
	return k
}

func encodeMessageKey(hash uint16, count int64, value []byte) (uint16, []byte) {
	shard := utils.GetShard(value, uint64(hash))
	k := make([]byte, 2+8)
	binary.BigEndian.PutUint16(k, shard)
	binary.BigEndian.PutUint64(k[2:], uint64(count))
	return shard, k
}

func decodeMessageKey(v []byte) (int64, error) {
	if len(v) < 8 {
		return 0, xerror.ErrValueTooShort
	}
	count := binary.BigEndian.Uint64(v)
	return int64(count), nil
}

func encodeMessageValue(timestamp int64, value []byte) []byte {
	k := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(k, uint64(timestamp))
	copy(k[8:], value)
	return k
}

func decodeMessageValue(v []byte) (int64, []byte, error) {
	if len(v) <= 8 {
		return 0, nil, xerror.ErrValueTooShort
	}
	timestamp := int64(binary.BigEndian.Uint64(v[:8]))
	value := v[8:]
	return timestamp, value, nil
}

func (c *Command) FPubSubHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetError(xerror.WrongArgsError(PUBSUB_COMMAND))
	}
	subcommand := strings.ToLower(utils.B2S(args[0]))
	switch subcommand {
	case "channels":
		return c.fpubSubChannels(txn, args[1:])
	default:
		return txn.SetWrongSubArgs(PUBSUB_COMMAND, PUBSUB_HELP)
	}
}

// PUBSUB CHANNELS [pattern] [COUNT count] [CURSOR cursor]
func (c *Command) fpubSubChannels(txn *store.Txn, args [][]byte) interface{} {
	pattern := ""
	if len(args)%2 != 0 {
		pattern = utils.B2S(args[0])
		args = args[1:]
	}

	count := c.cfg.Key.ScanMaxCount
	var cursor []byte
	var err error
	for i := 0; i < len(args); i += 2 {
		switch strings.ToLower(utils.B2S(args[i])) {
		case "count":
			count, err = utils.GetPositiveInt(args[i+1])
			if err != nil {
				return txn.SetError(xerror.ErrSyntax)
			}
			if count > c.cfg.Key.ScanMaxCount {
				count = c.cfg.Key.ScanMaxCount
			}
		case "cursor":
			cursor = args[i+1]
		default:
			return txn.SetError(xerror.ErrSyntax)
		}
	}
	prefix := GetDataPrefix(txn.UserId, PubSubDB, KeyType, nil)
	var start []byte
	if cursor != nil {
		start = GetDataPrefix(txn.UserId, PubSubDB, KeyType, cursor)
	} else if pattern != "" {
		cur := glob.Prefix(pattern)
		start = GetDataPrefix(txn.UserId, PubSubDB, KeyType, utils.S2B(cur))
	} else {
		start = prefix
	}
	end := utils.PrefixNext(prefix)
	channels := make([]string, 0)
	var matchError error
	utils.ZapLog.Debug("[txn] list", zap.String("remote", txn.RemoteAddr()),
		zap.Uint64("timestamp", txn.Timestamp), zap.ByteString("start", start), zap.ByteString("end", end))
	err = txn.List(start, end, count, func(k []byte, v []byte) bool {
		utils.ZapLog.Debug("[txn] list", zap.ByteString("key", k), zap.ByteString("value", v))
		object, err := GetObjectFromKV(k, v)
		if err != nil {
			return true
		}
		utils.ZapLog.Debug("[txn] list", zap.ByteString("object", object.Key))

		if object.Type != PubType {
			return true
		}

		if pattern != "" {
			var ok bool
			ok, matchError = glob.Match(pattern, utils.B2S(object.Key))
			if err != nil {
				// break
				return false
			}
			if !ok {
				// continue
				return true
			}
		}
		// count, err := c.GetCount(txn, PubType, 0, object.Key, nil)
		// if err != nil {
		// 	return false
		// }
		channels = append(channels, utils.B2S(object.Key))
		return true
	})
	if err != nil {
		return txn.SetError(err)
	}

	if matchError != nil {
		return txn.SetError(matchError)
	}
	return channels
}

// (pubsub) FPUBLISH channel message [PT partition] [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp]
func (c *Command) FPublishHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetError(xerror.WrongArgsError(FPUBLISH_COMMAND))
	}
	return c.fpublish(txn, args, true)
}

func (c *Command) fpublish(txn *store.Txn, args [][]byte, needOffset bool) interface{} {
	partition := ALLPartition
	expireStart := 2
	var err error
	if len(args) >= 4 {
		str := strings.ToLower(utils.B2S(args[2]))
		if str == "pt" {
			partition, err = getPartition(args[3])
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
			expireStart = 4
		}
	}

	nowTime := txn.NowTime()
	opt, err := checkExpireOption(PUBLISH_COMMAND, args[expireStart:], false, nowTime)
	if err != nil {
		return txn.SetError(err)
	}

	channel := args[0]
	str := strings.ToLower(utils.B2S(channel))
	if str == "pt" || str == "offset" {
		return txn.SetError(xerror.InvalidChannel)
	}

	object := NewObject(txn.UserId, PubSubDB, PubType, channel)
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		id, err := uuid.NewUUID()
		if err != nil {
			return txn.SetError(err)
		}
		object.Value = id[:]
	} else if err != nil {
		return txn.SetError(err)
	}

	if partition > int64(object.Hash) {
		return txn.SetError(xerror.InvalidPartition)
	}

	oldTTL := object.ValueTTL
	if opt.Expire > 0 {
		object.ValueTTL = opt.Expire
	} else if object.ValueTTL == 0 {
		object.ValueTTL = DefaultTTL
	}

	if object.ValueTTL != oldTTL {
		object.Timestamp = txn.Timestamp
		err = txn.Put(key, ObjectEncode(object))
		if err != nil {
			return txn.SetError(err)
		}
	}

	var msgCount *Count
	if partition == ALLPartition {
		msgCount, err = c.AddCount(txn, txn.UserId, MessageType, uint64(object.Hash), object.Value, args[1], 1)
	} else {
		msgCount, err = c.AddCount(txn, txn.UserId, MessageType, uint64(partition), object.Value, nil, 1)
	}
	if err != nil {
		return txn.SetError(err)
	}
	utils.ZapLog.Debug("PUBLISH", zap.ByteString("channel", channel), zap.ByteString("message", args[1]), zap.Int64("count", msgCount.Value))
	var mkey []byte
	if partition == ALLPartition {
		pt, key := encodeMessageKey(object.Hash, msgCount.Value, args[1])
		partition = int64(pt)
		mkey = object.GetKeyFieldBytes(key)
	} else {
		mkey = object.GetKeyFieldBytes(encodeMessageKeyOffset(uint16(partition), msgCount.Value))
	}

	err = txn.Put(mkey, encodeMessageValue(txn.Now, args[1]))
	if err != nil {
		return txn.SetError(err)
	}

	count, err := c.GetCount(txn, txn.UserId, PubType, 0, channel, nil)
	if err != nil && err != store.KeyNotFound {
		return txn.SetError(err)
	}
	if !needOffset {
		return SimpleInt(count.Value)
	} else {
		return []interface{}{SimpleInt(partition), SimpleInt(msgCount.Value)}
	}
}

func getPartition(arg []byte) (int64, error) {
	partition, err := strconv.ParseInt(utils.B2S(arg), 10, 64)
	if err != nil {
		return 0, err
	}

	if partition < 0 {
		partition = ALLPartition
	}
	return partition, nil
}

type FSubOpt struct {
	offset int64
	limit  int
}

func checkFSub(args [][]byte) (FSubOpt, error) {
	opt := FSubOpt{
		offset: NoOffset,
		limit:  100,
	}
	for i := 1; i < len(args); i += 2 {
		str := strings.ToLower(utils.B2S(args[i]))
		switch str {
		case "offset":
			offset, err := utils.GetNonnegativeInt64(args[i+1])
			if err != nil {
				return opt, xerror.InvalidOffset
			}
			opt.offset = offset
		case "limit":
			limit, err := utils.GetPositiveInt(args[i+1])
			if err != nil {
				return opt, xerror.InvalidLimit
			}
			opt.limit = limit
		}
	}
	return opt, nil
}

type Message struct {
	Millisecond int64
	Value       []byte
	Offset      int64
}

// FSUBSCRIBE channel partition [OFFSET offset] [LIMIT limit]
func (c *Command) FSubscribeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 || len(args)%2 != 0 {
		return txn.SetError(xerror.WrongArgsError(FSUBSCRIBE_COMMAND))
	}

	pt, err := utils.GetNonnegativeInt64(args[1])
	if err != nil {
		return txn.SetError(xerror.InvalidPartition)
	}
	partition := uint16(pt)

	opt, err := checkFSub(args[1:])
	if err != nil {
		return txn.SetError(err)
	}
	channel := args[0]
	object := NewObject(txn.UserId, PubSubDB, PubType, channel)
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return err
	}
	if partition > object.Hash {
		return txn.SetError(xerror.InvalidPartition)
	}

	prefix := object.GetValueBytesPrefix(encodeMessageKeyPrefix(partition))
	var start []byte
	if opt.offset == NoOffset {
		start = prefix
	} else {
		start = object.GetValueBytesPrefix(encodeMessageKeyOffset(partition, opt.offset))
	}
	end := utils.PrefixNext(prefix)
	var lastKey []byte
	count := 0

	messages := make([][]interface{}, 0)
	callback := func(key, value []byte) bool {
		lastKey = key
		count++
		timestamp, message, err := decodeMessageValue(value)
		if err != nil {
			return true
		}
		if len(key) <= len(prefix) {
			return true
		}
		newOffset, err := decodeMessageKey(key[len(prefix):])
		if err != nil {
			return true
		}
		messages = append(messages, []interface{}{message, timestamp, newOffset})
		return true
	}
	err = txn.List(start, end, opt.limit, callback)
	if lastKey != nil {
		lastKey = utils.NextKey(lastKey)
		utils.ZapLog.Debug("FSUBSCRIBE", zap.ByteString("channel", channel),
			zap.Uint16("partition", partition), zap.Int("count", count), zap.ByteString("next", lastKey))
	}
	if err != nil {
		return txn.SetError(err)
	}
	if len(messages) == 0 {
		return nil
	}
	return messages
}
