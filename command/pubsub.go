package command

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/utils/glob"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const (
	DefaultTTL       = 30 * 60 * 1000 // 30 minutes
	PubSubDB   uint8 = 200

	NoOffset     int64 = -1
	ALLPartition int64 = -1
	PUBSUB_HELP        = "PUBSUB HELP"
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

func (c *Command) PubSubHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetError(xerror.WrongArgsError(PUBSUB_COMMAND))
	}
	subcommand := strings.ToLower(utils.B2S(args[0]))
	switch subcommand {
	case "channels":
		return c.pubSubChannels(txn, args[1:])
	case "numsub":
		return c.pubSubNumsub(txn, args[1:])
	default:
		return txn.SetWrongSubArgs(PUBSUB_COMMAND, PUBSUB_HELP)
	}
}

// PUBSUB CHANNELS [pattern] [COUNT count] [CURSOR cursor]
func (c *Command) pubSubChannels(txn *store.Txn, args [][]byte) interface{} {
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

// PUBSUB NUMSUB [channel-1 ... channel-N]
func (c *Command) pubSubNumsub(txn *store.Txn, args [][]byte) interface{} {
	if len(args) == 0 {
		return nil
	}
	rets := make([]interface{}, 0)
	for i := 0; i < len(args); i++ {
		count, err := c.GetCount(txn, txn.UserId, PubType, 0, args[i], nil)
		if err != nil && err != store.KeyNotFound {
			return txn.SetError(err)
		}
		rets = append(rets, args[i], SimpleInt(count.Value))
	}
	return rets
}

// (pubsub) FPUBLISH channel message [PT partition] [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp]
func (c *Command) FPublishHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetError(xerror.WrongArgsError(FPUBLISH_COMMAND))
	}
	return c.publish(txn, args, true)
}

// (pubsub) PUBLISH channel message [PT partition] [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp]
func (c *Command) PublishHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetError(xerror.WrongArgsError(PUBLISH_COMMAND))
	}
	return c.publish(txn, args, false)
}

func (c *Command) publish(txn *store.Txn, args [][]byte, needOffset bool) interface{} {
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

type pubSubConn struct {
	sync.RWMutex
	dconn    *redcon.DetachedConn
	channels map[string]map[uint16][]byte
	messages chan []interface{}
	others   chan interface{}
	closed   chan struct{}
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

// FSUBSCRIBE partition offset channel [channel ...]
func (c *Command) fsubscribe(conn *redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 4 {
		conn.WriteError(xerror.WrongArgsString(FSUBSCRIBE_COMMAND))
		return
	}

	partition, err := getPartition(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	if partition < 0 {
		partition = ALLPartition
	}

	offset, err := utils.GetNonnegativeInt64(cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	utils.ZapLog.Debug("fsubscribe", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	c._subscribe(conn, cmd.Args[2:], partition, offset, true)
}

// SUBSCRIBE channel [channel ...] [PT partition] [OFFSET offset]
func (c *Command) subscribe(conn *redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError(xerror.WrongArgsString(SUBSCRIBE_COMMAND))
		return
	}

	channels := make([][]byte, 0)
	partition := ALLPartition
	offset := NoOffset
	var err error
	for i := 1; i < len(cmd.Args); i++ {
		str := strings.ToLower(utils.B2S(cmd.Args[i]))
		switch str {
		case "pt":
			if i+1 >= len(cmd.Args) {
				conn.WriteError(xerror.WrongArgsString(SUBSCRIBE_COMMAND))
				return
			}
			partition, err = getPartition(cmd.Args[i+1])
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			i++
		case "offset":
			if i+1 >= len(cmd.Args) {
				conn.WriteError(xerror.WrongArgsString(SUBSCRIBE_COMMAND))
				return
			}
			offset, err = utils.GetNonnegativeInt64(cmd.Args[i+1])
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			i++
		default:
			channels = append(channels, cmd.Args[i])
		}
	}

	utils.ZapLog.Debug("subscribe", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	c._subscribe(conn, channels, partition, offset, false)
}

func (c *Command) _subscribe(conn *redcon.Conn, args [][]byte, partition int64, offset int64, needOffset bool) {
	ps := &pubSubConn{
		dconn:    &redcon.DetachedConn{Conn: conn},
		channels: make(map[string]map[uint16][]byte),
		messages: make(chan []interface{}, 1000),
		others:   make(chan interface{}, 2),
		closed:   make(chan struct{}),
	}
	err := c.subDetached(ps, args, partition, offset)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	ps.dconn = conn.Detach()
	go ps.sendMessage()
	go c.runDetached(ps)
	go c.Pull(ps, needOffset)
}

// FUNSUBSCRIBE partition offset channel [channel ...]
func (c *Command) funsubscribe(conn *redcon.Conn, cmd redcon.Command) {
}

// UNSUBSCRIBE channel [channel ...]
func (c *Command) unsubscribe(conn *redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 1 {
		conn.WriteArray(3)
		conn.WriteBulkString("unsubscribe")
		conn.WriteBulkString("all")
		conn.WriteInt(0)
		return
	}
	for _, resp := range cmd.Args[1:] {
		conn.WriteArray(3)
		conn.WriteBulkString("unsubscribe")
		conn.WriteBulk(resp)
		conn.WriteInt(0)
	}
}

func (c *Command) tryDeleteConnCount(userID uint16, channels []string) {
	for i := 0; i < 3; i++ {
		err := c.deleteConnCount(userID, channels)
		if err != nil {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		return
	}
}

func (c *Command) deleteConnCount(userID uint16, channels []string) error {
	txn := c.client.NewTxn()
	defer txn.Rollback()
	err := txn.Begin()
	if err != nil {
		return err
	}
	for _, channel := range channels {
		_, err = c.AddCount(txn, userID, PubType, 0, utils.S2B(channel), nil, -1)
		if err != nil {
			return err
		}
	}
	return txn.Commit()
}

func (conn *pubSubConn) Close(c *Command) error {
	close(conn.closed)
	conn.dconn.Close()
	conn.Lock()
	var channels []string
	for k := range conn.channels {
		channels = append(channels, k)
	}
	conn.Unlock()
	c.tryDeleteConnCount(conn.dconn.UserId, channels)
	return nil
}

func (conn *pubSubConn) sendMessage() {
	for {
		select {
		case msg := <-conn.messages:
			utils.ZapLog.Debug("message", zap.String("remote", conn.dconn.RemoteAddr()),
				zap.Any("message", msg))
			if msg[0] == "message" {
				var channel string
				var partition redcon.SimpleInt = -1
				if len(msg) > 1 {
					channel = msg[1].(string)
				}
				if len(msg) > 3 {
					partition = msg[2].(redcon.SimpleInt)
				}

				conn.RLock()
				ch, ok := conn.channels[channel]
				if !ok {
					conn.RUnlock()
					break
				}
				if partition > 0 {
					if _, ok := ch[uint16(partition)]; !ok {
						conn.RUnlock()
						break
					}
				}
				conn.RUnlock()
			}

			conn.dconn.WriteAny(msg)
			conn.dconn.Flush()
		case other := <-conn.others:
			conn.dconn.WriteAny(other)
			conn.dconn.Flush()
			if other == OK {
				return
			}
		case <-conn.closed:
			return
		}
	}
}

func (conn *pubSubConn) isChannelExist(c string) bool {
	conn.RLock()
	_, ok := conn.channels[c]
	conn.RUnlock()
	return ok
}

func (conn *pubSubConn) Channels() []string {
	conn.RLock()
	defer conn.RUnlock()
	var keys []string
	for k := range conn.channels {
		keys = append(keys, k)
	}
	return keys
}

func (c *Command) unsubDetached(conn *pubSubConn, args [][]byte) {
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		conn.others <- err
		return
	}
	defer txn.Rollback()

	all := false
	var channels []string
	if len(args) == 0 {
		all = true
		channels = conn.Channels()
	} else {
		channels = make([]string, len(args))
		for i, arg := range args {
			channels[i] = string(arg)
		}
	}
	resps := make([]string, len(args))
	argsCh := make(map[string]bool)
	for i, ch := range channels {
		if conn.isChannelExist(ch) && !argsCh[ch] {
			argsCh[ch] = true
			object := NewObject(conn.dconn.UserId, PubSubDB, PubType, utils.S2B(ch))
			key := object.GetKeyBytes()
			err := getTxnObject(txn, key, object, true)
			if err == store.KeyNotFound {
			} else if err != nil {
				conn.others <- err
				return
			} else {
				_, err = c.AddCount(txn, conn.dconn.UserId, PubType, 0, utils.S2B(ch), nil, -1)
				if err != nil {
					conn.others <- err
					return
				}
			}
		}
		if !all {
			resps[i] = ch
		}
	}
	err = txn.Commit()
	if err != nil {
		conn.others <- err
		return
	}

	if all {
		conn.Lock()
		for c := range conn.channels {
			delete(conn.channels, c)
		}
		conn.Unlock()
		conn.messages <- []interface{}{"unsubscribe", "all", SimpleInt(0)}
	} else {
		for _, channel := range resps {
			conn.Lock()
			delete(conn.channels, channel)
			count := len(conn.channels)
			conn.Unlock()
			conn.messages <- []interface{}{"unsubscribe", channel, SimpleInt(int64(count))}
		}
	}
}

func (c *Command) subDetached(conn *pubSubConn, args [][]byte, partition, offset int64) error {
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()
	txn.Conn = conn.dconn.Conn

	resps := make([]func(), len(args))
	argsCh := make(map[string]bool)
	for i, ch := range args {
		cha := string(ch)
		if !conn.isChannelExist(cha) && !argsCh[cha] {
			argsCh[cha] = true
			object := NewObject(txn.UserId, PubSubDB, PubType, ch)
			key := object.GetKeyBytes()
			err := getTxnObject(txn, key, object, true)
			if err == store.KeyNotFound {
				var id uuid.UUID
				id, err = uuid.NewUUID()
				if err != nil {
					return err
				}
				object.Value = id[:]
				object.ValueTTL = DefaultTTL
				object.Timestamp = txn.Timestamp
				err = txn.Put(key, ObjectEncode(object))
			}

			if err != nil {
				return err
			}

			if partition > int64(object.Hash) {
				return xerror.InvalidPartition
			}

			_, err = c.AddCount(txn, txn.UserId, PubType, 0, ch, nil, 1)
			if err != nil {
				return err
			}
			var start []byte
			var j uint16
			channelHash := make(map[uint16][]byte)
			if partition == ALLPartition {
				for j = 0; j < object.Hash; j++ {
					if offset == NoOffset {
						channelHash[j] = object.GetValueBytesPrefix(encodeMessageKeyPrefix(j))
					} else {
						channelHash[j] = object.GetValueBytesPrefix(encodeMessageKeyOffset(j, offset))
					}
					utils.ZapLog.Debug("channel start",
						zap.String("channel", cha), zap.Uint16("partition", j),
						zap.ByteString("start", channelHash[j]))
				}
			} else {
				p := uint16(partition)
				if offset == NoOffset {
					channelHash[p] = object.GetValueBytesPrefix(encodeMessageKeyPrefix(p))
				} else {
					channelHash[p] = object.GetValueBytesPrefix(encodeMessageKeyOffset(p, offset))
				}
				utils.ZapLog.Debug("channel start",
					zap.String("channel", cha), zap.Uint16("partition", p),
					zap.ByteString("start", channelHash[p]))
			}

			resps[i] = func() {
				conn.Lock()
				conn.channels[cha] = channelHash
				count := len(conn.channels)
				conn.Unlock()
				utils.ZapLog.Debug("subscribe", zap.String("channel", cha), zap.ByteString("next", start))
				conn.messages <- []interface{}{"subscribe", cha, SimpleInt(int64(count))}
			}
		} else {
			resps[i] = func() {
				conn.RLock()
				count := len(conn.channels)
				conn.RUnlock()
				conn.messages <- []interface{}{"subscribe", cha, SimpleInt(int64(count))}
			}
		}
	}
	err = txn.Commit()
	if err != nil {
		return err
	}

	for _, resp := range resps {
		resp()
	}
	return nil
}

func (c *Command) Pull(conn *pubSubConn, needOffset bool) {
	c.pullMessages(conn, needOffset)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.pullMessages(conn, needOffset)
		case <-conn.closed:
			return
		}
	}
}

func (c *Command) pullMessages(sconn *pubSubConn, needOffset bool) {
	chans := make(map[string]map[uint16][]byte)
	sconn.RLock()
	for c, partitons := range sconn.channels {
		newPartitions := make(map[uint16][]byte)
		for partiton, start := range partitons {
			newPartitions[partiton] = start
		}
		chans[c] = newPartitions
	}
	sconn.RUnlock()
	wg := &sync.WaitGroup{}
	for ch, partitions := range chans {
		wg.Add(1)
		go func(channel string, partitions map[uint16][]byte) {
			_ = c.pullMessgeFromChannel(sconn, channel, partitions, 100, needOffset)
			wg.Done()
		}(ch, partitions)
	}
	wg.Wait()
}

func (c *Command) pullMessgeFromChannel(conn *pubSubConn, channel string,
	partitions map[uint16][]byte, limit int, needOffset bool) error {
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		return err
	}
	txn.Conn = conn.dconn.Conn

	object := NewObject(txn.UserId, PubSubDB, PubType, utils.S2B(channel))
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return err
	}
	for len(partitions) > 0 {
		for partition, start := range partitions {
			select {
			case <-conn.closed:
				return xerror.ErrClosed
			default:
			}

			lastKey, next, err := c.pullMessgeFromPartition(conn, txn, object, channel, partition, start, limit, needOffset)
			if next {
				if next {
					utils.ZapLog.Debug("pull message next", zap.String("channel", channel),
						zap.Uint16("partition", partition), zap.ByteString("lastKey", lastKey))
					partitions[partition] = lastKey
					continue
				}
			}
			delete(partitions, partition)
			if err == nil {
				continue
			}

			tick := time.NewTicker(time.Millisecond * 100)
			select {
			case <-conn.closed:
				return xerror.ErrClosed
			case <-tick.C:
			}
		}
	}

	return nil
}

func (c *Command) pullMessgeFromPartition(conn *pubSubConn, txn *store.Txn, object *Object,
	channel string, partition uint16, start []byte, limit int, needOffset bool) ([]byte, bool, error) {
	prefix := object.GetValueBytesPrefix(encodeMessageKeyPrefix(partition))
	end := utils.PrefixNext(prefix)
	var lastKey []byte
	next := false
	count := 0
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

		if math.Abs(float64(timestamp-txn.Now)) < 1000 {
			next = true
		}
		if !needOffset {
			conn.messages <- []interface{}{"message", channel, message}
		} else {
			conn.messages <- []interface{}{"message", channel, SimpleInt(int64(partition)),
				SimpleInt(int64(newOffset)), message}
		}
		return true
	}
	err := txn.List(start, end, limit, callback)
	if lastKey != nil {
		lastKey = utils.NextKey(lastKey)
		utils.ZapLog.Debug("pullMessgeFromPartition", zap.String("channel", channel),
			zap.Uint16("partition", partition), zap.Int("count", count), zap.ByteString("next", lastKey))
		conn.Lock()
		if ch, ok := conn.channels[channel]; ok {
			if _, ok := ch[partition]; ok {
				ch[partition] = lastKey
			}
		}
		conn.Unlock()
	}

	if err == store.ReachLimit {
		return lastKey, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	return lastKey, next, err
}

func (c *Command) runDetached(sconn *pubSubConn) {
	defer func() {
		sconn.Close(c)
	}()
	for {
		cmd, err := sconn.dconn.ReadCommand()
		if err != nil {
			return
		}
		if len(cmd.Args) == 0 {
			continue
		}
		utils.ZapLog.Debug("detached", zap.String("remote", sconn.dconn.RemoteAddr()),
			zap.ByteStrings("args", cmd.Args))
		comma := string(cmd.Args[0])
		switch strings.ToLower(comma) {
		case "subscribe":
			if len(cmd.Args) < 2 {
				sconn.others <- xerror.WrongArgsError(SUBSCRIBE_COMMAND)
				continue
			}
			err = c.subDetached(sconn, cmd.Args[1:], ALLPartition, NoOffset)
			if err != nil {
				sconn.others <- err
				continue
			}
		case "fsubscribe":
			if len(cmd.Args) != 4 {
				sconn.others <- xerror.WrongArgsError(FSUBSCRIBE_COMMAND)
				continue
			}
			partition, err := getPartition(cmd.Args[1])
			if err != nil {
				sconn.others <- err
				return
			}
			offset, err := utils.GetNonnegativeInt64(cmd.Args[1])
			if err != nil {
				sconn.others <- err
				return
			}
			err = c.subDetached(sconn, cmd.Args[2:], partition, offset)
			if err != nil {
				sconn.others <- err
				continue
			}
		case "unsubscribe":
			c.unsubDetached(sconn, cmd.Args[1:])
		case "funsubscribe":
		case "quit":
			sconn.others <- OK
			return
		case "ping":
			var msg string
			switch len(cmd.Args) {
			case 1:
			case 2:
				msg = string(cmd.Args[1])
			default:
				sconn.others <- xerror.WrongArgsError(PING_COMMAND)
				continue
			}
			sconn.messages <- []interface{}{"pong", msg}
		default:
			sconn.others <- xerror.InvalidCommandError(comma)
		}
	}
}
