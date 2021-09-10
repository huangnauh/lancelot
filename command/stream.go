package command

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/utils/glob"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const (
	DefaultTTL = 30 * 60 * 1000 // 30 minutes

	NOMKSTREAM   = "nomkstream"
	MKSTREAM     = "mkstream"
	MAXLEN       = "maxlen"
	MINID        = "minid"
	LIMIT        = "limit"
	DefaultEXACT = 0
	ALLMOSTEXACT = 1
	EXACT        = 2

	NotFound   int64 = -1
	Auto       int64 = -2
	StartIndex int64 = -3
	EndIndex   int64 = -4

	NoLimit      int64 = -1
	NoOffset     int64 = -1
	ALLPartition int64 = -1
	DefaultLIMIT int64 = 100
	MaxLimit     int64 = 1000
	XGROUP_HELP        = "XGROUP HELP"
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

func encodeMessageKey(hash uint16, count uint64, value []byte) (uint16, []byte) {
	shard := utils.GetShard(value, uint64(hash))
	k := make([]byte, 2+8)
	binary.BigEndian.PutUint16(k, shard)
	binary.BigEndian.PutUint64(k[2:], count)
	return shard, k
}

func decodeMessageKey(v []byte) (uint16, int64, error) {
	if len(v) < 2+8 {
		return 0, 0, xerror.ErrValueTooShort
	}
	partition := binary.BigEndian.Uint16(v)
	offset := int64(binary.BigEndian.Uint64(v[2:]))
	return partition, offset, nil
}

func encodeMessageValue(timestamp int64, count int64, value []byte) []byte {
	k := make([]byte, 8+8+len(value))
	binary.BigEndian.PutUint64(k, uint64(timestamp))
	binary.BigEndian.PutUint64(k[8:], uint64(count))
	copy(k[16:], value)
	return k
}

func decodeMessageValue(v []byte) (int64, int64, []byte, error) {
	if len(v) <= 8+8 {
		return 0, 0, nil, xerror.ErrValueTooShort
	}
	timestamp := int64(binary.BigEndian.Uint64(v[:8]))
	count := int64(binary.BigEndian.Uint64(v[8:]))
	value := v[16:]
	return timestamp, count, value, nil
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
	prefix := GetKeyBytes(DataPrefix, txn.UserId, txn.DBId, KeyPrefix, nil)
	var start []byte
	if cursor != nil {
		start = GetKeyBytes(DataPrefix, txn.UserId, txn.DBId, KeyPrefix, cursor)
	} else if pattern != "" {
		cur := glob.Prefix(pattern)
		start = GetKeyBytes(DataPrefix, txn.UserId, txn.DBId, KeyPrefix, utils.S2B(cur))
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

		if object.Type != StreamType {
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
		// count, err := c.GetCount(txn, StreamType, 0, object.Key, nil)
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

func getThreshold(args [][]byte) (exact int, threshold int64, err error) {
	if len(args) == 0 {
		err = xerror.ErrSyntax
		return
	}

	i := 0
	exact = DefaultEXACT
	arg := utils.B2S(args[i])
	if arg == "~" {
		exact = ALLMOSTEXACT
		i++
	} else if arg == "=" {
		exact = EXACT
		i++
	}

	if i >= len(args) {
		err = xerror.ErrSyntax
		return
	}

	threshold, err = utils.GetNonnegativeInt64(args[i])
	if err != nil {
		err = xerror.ErrNotInteger
		return
	}
	return
}

func DecodeStream(b []byte) ([][]byte, error) {
	args := make([][]byte, 0)
	err := msgpack.Unmarshal(b, &args)
	return args, err
}

func EncodeStream(args [][]byte) ([]byte, error) {
	return msgpack.Marshal(args)
}

func checkID(arg []byte) (int64, int64, error) {
	id := strings.ToLower(utils.B2S(arg))
	if id == "*" {
		return Auto, NotFound, nil
	}
	if id == "-" {
		return StartIndex, NotFound, nil
	}
	if id == "+" {
		return EndIndex, NotFound, nil
	}

	ids := strings.Split(id, "-")
	if len(ids) != 1 && len(ids) != 2 {
		return 0, 0, xerror.InvalidStreamID
	}

	first, err := strconv.ParseInt(ids[0], 10, 64)
	if err != nil || first < 0 {
		return 0, 0, xerror.InvalidStreamID
	}
	if len(ids) == 1 {
		return first, NotFound, nil
	}
	second, err := strconv.ParseInt(ids[1], 10, 64)
	if err != nil || second < 0 {
		return 0, 0, xerror.InvalidStreamID
	}
	return first, second, nil
}

// (stream) XGROUP
func (c *Command) XgroupHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetError(xerror.WrongArgsError(XGROUP_COMMAND))
	}
	subCommand := strings.ToLower(utils.B2S(args[0]))
	switch subCommand {
	case CREATE_COMMAND:
	case DESTORY_COMMAND:
	case SETID_COMMAND:
	case CREATECONSUMER:
	case DELCONSUMER:
	default:
		return txn.SetWrongSubArgs(XGROUP_COMMAND, XGROUP_HELP)
	}
	return nil
}

func (c *Command) CreateStream(txn *store.Txn, key []byte, object *Object) error {
	id, err := uuid.NewUUID()
	if err != nil {
		return txn.SetError(err)
	}
	object.Value = id[:]
	return txn.Put(key, ObjectEncode(object))
}

type Consumer struct {
	Group      string
	Offset     []int64
	Watermark  []int64
	Partitions []uint16
}

func EncodeConsumer(c *Consumer) ([]byte, error) {
	return msgpack.Marshal(c)
}

func DecodeConsumer(b []byte) (*Consumer, error) {
	var c Consumer
	err := msgpack.Unmarshal(b, &c)
	return &c, err
}

type Group struct {
	Name      string
	Consumers uint16
	Timestamp int64
	Consumer  []Consumer
}

func EncodeGroup(group *Group) []byte {
	k := make([]byte, 6)
	binary.BigEndian.PutUint32(k, uint32(group.Timestamp))
	binary.BigEndian.PutUint16(k[4:], group.Consumers)
	return k
}

func DecodeGroup(k []byte) *Group {
	group := &Group{}
	group.Timestamp = int64(binary.BigEndian.Uint32(k))
	group.Consumers = binary.BigEndian.Uint16(k[4:])
	return group
}

// (stream) XGROUP CREATE key groupname ID|$ [MKSTREAM]
func (c *Command) XCreateHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetError(xerror.WrongSubArgsError(CREATE_COMMAND, XGROUP_HELP))
	}
	// id := args[2]
	create := false
	if len(args) > 3 {
		if strings.ToLower(utils.B2S(args[3])) != MKSTREAM {
			return txn.SetError(xerror.ErrSyntax)
		}
		create = true
	}

	object := NewObject(txn.UserId, txn.DBId, StreamType, args[0])
	key := object.GetKeyBytes()
	err := c.getTxnObject(txn, key, object, create)
	if err == store.KeyNotFound {
		if !create {
			return txn.SetError(xerror.XgroupRequireExist)
		}
		err = c.CreateStream(txn, key, object)
	}

	if err != nil {
		return txn.SetError(err)
	}

	gobject := NewObject(txn.UserId, txn.DBId, GroupType, args[1])
	key = object.GetKeyBytes()
	err = c.getTxnObject(txn, key, gobject, true)
	if err == nil {
		return txn.SetError(xerror.XgroupAlreadyExist)
	}
	if err != store.KeyNotFound {
		return txn.SetError(err)
	}
	// gobject.Value =

	return nil
}

// (stream) XRANGE key start end [COUNT count]
func (c *Command) XRangeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetError(xerror.WrongArgsError(XRANGE_COMMAND))
	}
	partition := ALLPartition
	var startOffset int64
	endOffset := NoOffset
	first, second, err := checkID(args[1])
	if err != nil {
		return txn.SetError(err)
	}

	if first < 0 && first != StartIndex {
		return txn.SetError(xerror.InvalidStreamID)
	} else if first >= 0 {
		partition = first
	}
	if second >= 0 {
		startOffset = second
	}

	first, second, err = checkID(args[2])
	if err != nil {
		return txn.SetError(err)
	}
	if first < 0 && first != EndIndex {
		return txn.SetError(xerror.InvalidStreamID)
	} else if first >= 0 {
		if partition >= 0 && partition != first {
			return txn.SetError(xerror.InvalidStreamID)
		}
		partition = first
	}

	if second >= 0 {
		endOffset = second
	}

	if startOffset >= 0 && endOffset >= 0 && endOffset < startOffset {
		return EmptySlice
	}

	count := DefaultLIMIT
	if len(args) > 3 {
		str := strings.ToLower(utils.B2S(args[3]))
		switch str {
		case "count":
			if len(args) < 5 {
				return txn.SetError(xerror.WrongArgsError(XADD_COMMAND))
			}
			count, err = utils.GetNonnegativeInt64(args[4])
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
			if count > MaxLimit {
				count = MaxLimit
			}
		default:
			return txn.SetError(xerror.ErrSyntax)
		}
	}

	if count == 0 {
		return EmptySlice
	}

	object := NewObject(txn.UserId, txn.DBId, StreamType, args[0])
	key := object.GetKeyBytes()
	err = c.getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return EmptySlice
	}

	if err != nil {
		return txn.SetError(err)
	}

	if partition > int64(object.Hash) {
		return txn.SetError(xerror.InvalidPartition)
	}

	prefix := object.GetValueBytes(nil)
	var start, end []byte
	if partition < 0 {
		start = prefix
		end = utils.PrefixNext(prefix)
	} else {
		if startOffset < 0 {
			start = object.GetValueBytes(encodeMessageKeyPrefix(uint16(partition)))
		} else {
			start = object.GetValueBytes(encodeMessageKeyOffset(uint16(partition), startOffset))
		}
		if endOffset < 0 {
			end = utils.PrefixNext(object.GetValueBytes(encodeMessageKeyPrefix(uint16(partition))))
		} else {
			end = object.GetValueBytes(encodeMessageKeyOffset(uint16(partition), endOffset+1))
		}
	}
	result := make([][]interface{}, 0)
	sum := 0
	err = txn.List(start, end, int(count), func(k, v []byte) bool {
		sum++
		pt, of, err := decodeMessageKey(k[len(prefix):])
		if err != nil {
			return true
		}
		id := fmt.Sprintf("%d-%d", pt, of)
		_, _, value, err := decodeMessageValue(v)
		if err != nil {
			return true
		}
		vv, err := DecodeStream(value)
		if err != nil {
			return true
		}
		result = append(result, []interface{}{id, vv})
		return true
	})
	utils.ZapLog.Debug("XRangeHandle", zap.ByteString("start", start), zap.ByteString("end", end),
		zap.Int64("start offset", startOffset), zap.Int64("end offset", endOffset),
		zap.Int("list", sum), zap.Int64("limit", count), zap.Int64("partition", partition), zap.Error(err))

	if err != nil {
		return txn.SetError(err)
	}
	return result
}

// (stream) XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|ID field value [field value ...]
func (c *Command) XADDHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 4 {
		return txn.SetError(xerror.WrongArgsError(XADD_COMMAND))
	}

	checkExist := false
	maxLen := NoLimit
	var minId []byte
	minPartition := ALLPartition
	minOffset := NoOffset
	exact := DefaultEXACT
	limit := DefaultLIMIT
	var err error
	var i int

LOOP:
	for i = 1; i < len(args); i++ {
		arg := strings.ToLower(utils.B2S(args[1]))
		switch arg {
		case NOMKSTREAM:
			checkExist = true
			continue
		case MINID:
			i++
			if maxLen != NoLimit || minId != nil {
				return txn.SetError(xerror.ErrSyntax)
			}
			minId = args[i]
			first, second, err := checkID(minId)
			if err != nil {
				return txn.SetError(err)
			}
			//TODO?
			if first < 0 {
				return txn.SetError(xerror.InvalidStreamID)
			}

			if second == NotFound {
				minOffset = first
			} else {
				minPartition = first
				minOffset = second
			}
		case MAXLEN:
			i++
			if maxLen != NoLimit || minId != nil {
				return txn.SetError(xerror.ErrSyntax)
			}

			exact, maxLen, err = getThreshold(args[i:])
			if err != nil {
				return txn.SetError(err)
			}
			if exact != DefaultEXACT {
				i += 1
			}
		case LIMIT:
			i++
			limit, err = utils.GetNonnegativeInt64(args[i])
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}

			if limit > MaxLimit {
				limit = MaxLimit
			}
		default:
			break LOOP
		}
	}

	channel := args[0]
	partition := ALLPartition
	offset := NoOffset
	first, second, err := checkID(args[i])
	if err != nil {
		return txn.SetError(err)
	}
	if first >= 0 {
		partition = first
	}
	if second >= 0 {
		offset = second
	}

	objectValue, err := EncodeStream(args[i+1:])
	if err != nil {
		return txn.SetError(err)
	}

	object := NewObject(txn.UserId, txn.DBId, StreamType, channel)
	key := object.GetKeyBytes()
	err = c.getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		if checkExist {
			return nil
		}
		err = c.CreateStream(txn, key, object)
	}

	if err != nil {
		return txn.SetError(err)
	}

	if partition > int64(object.Hash) {
		return txn.SetError(xerror.InvalidPartition)
	}

	var msgCount *Count
	if partition == ALLPartition {
		msgCount, err = GetCount(txn, txn.UserId, txn.DBId, uint64(object.Hash), KeyPrefix, object.Value, objectValue)
	} else {
		msgCount, err = GetCount(txn, txn.UserId, txn.DBId, uint64(partition), KeyPrefix, object.Value, nil)
	}
	if err == store.KeyNotFound {
		msgCount.UserValue = make([]byte, 8)
	} else if err != nil {
		return txn.SetError(err)
	}

	if minPartition >= 0 && minPartition != int64(msgCount.Shard) {
		return txn.SetError(xerror.InvalidPartition)
	}

	msgCount.Value += 1

	var cur uint64
	if len(msgCount.UserValue) > 0 {
		cur = binary.BigEndian.Uint64(msgCount.UserValue)
	}
	cur += 1

	utils.ZapLog.Debug(XADD_COMMAND, zap.ByteString("key", channel),
		zap.Int64("count", msgCount.Value), zap.Uint64("user current", cur),
		zap.Int64("partition", partition), zap.Int64("offset", offset), zap.Int("exact", exact),
		zap.ByteString("minid", minId), zap.Int64("maxlen", maxLen), zap.Int64("limit", limit))

	if offset == NoOffset {
		binary.BigEndian.PutUint64(msgCount.UserValue, cur)
	} else {
		if uint64(offset) < cur {
			return txn.SetError(xerror.ErrXADDID)
		}
		binary.BigEndian.PutUint64(msgCount.UserValue, uint64(offset))
	}
	err = SetCount(txn, msgCount)
	if err != nil {
		return txn.SetError(err)
	}

	id := fmt.Sprintf("%d-%d", msgCount.Shard, msgCount.Value)
	mkey := object.GetValueBytes(encodeMessageKeyOffset(msgCount.Shard, msgCount.Value))
	err = txn.Put(mkey, encodeMessageValue(txn.Now, msgCount.Value, objectValue))
	if err != nil {
		return txn.SetError(err)
	}

	if minOffset >= 0 || maxLen >= 0 {
		start := object.GetValueBytes(encodeMessageKeyPrefix(msgCount.Shard))
		var end []byte
		var max int64
		if minOffset >= 0 {
			end = object.GetValueBytes(encodeMessageKeyOffset(msgCount.Shard, minOffset))
		} else {
			if msgCount.Value <= maxLen {
				return id
			}
			max = msgCount.Value - maxLen
			end = utils.PrefixNext(start)
		}

		go func(start, end []byte, max int64, limit int) {
			count := 0
			_, _, _ = c.client.DeleteUntil(start, end, limit, func(k, v []byte) bool {
				count++
				_, msgCount, _, err := decodeMessageValue(v)
				if err != nil {
					return true
				}
				if max > 0 && max <= msgCount {
					return false
				}
				return true
			})
			utils.ZapLog.Debug(XADD_COMMAND, zap.ByteString("key", channel), zap.Int("deleted", count),
				zap.ByteString("start", start), zap.ByteString("end", end), zap.Int64("max", max), zap.Int("limit", limit))
		}(start, end, max, int(limit))
	}

	return id
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
	object := NewObject(txn.UserId, txn.DBId, StreamType, channel)
	key := object.GetKeyBytes()
	err = c.getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return err
	}
	if partition > object.Hash {
		return txn.SetError(xerror.InvalidPartition)
	}

	p := object.GetValueBytes(nil)
	prefix := object.GetValueBytes(encodeMessageKeyPrefix(partition))
	var start []byte
	if opt.offset == NoOffset {
		start = prefix
	} else {
		start = object.GetValueBytes(encodeMessageKeyOffset(partition, opt.offset))
	}
	end := utils.PrefixNext(prefix)
	var lastKey []byte
	count := 0

	messages := make([][]interface{}, 0)
	callback := func(key, value []byte) bool {
		lastKey = key
		count++
		timestamp, _, message, err := decodeMessageValue(value)
		if err != nil {
			return true
		}
		if len(key) <= len(prefix) {
			return true
		}
		_, newOffset, err := decodeMessageKey(key[len(p):])
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
