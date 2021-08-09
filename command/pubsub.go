package command

import (
	"encoding/binary"
	"math"
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

	NoOffset    int64 = -1
	PUBSUB_HELP       = "PUBSUB HELP"
)

func encodeMessageKey(count int64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(count))
	return k
}

func decodeMessageKey(v []byte) (int64, error) {
	if len(v) != 8 {
		return 0, xerror.ErrValueTooShort
	}
	return int64(binary.BigEndian.Uint64(v)), nil
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
		count, err := c.GetCount(txn, PubType, 0, args[i], nil)
		if err != nil && err != store.KeyNotFound {
			return txn.SetError(err)
		}
		rets = append(rets, args[i], SimpleInt(count.Value))
	}
	return rets
}

// (pubsub) PUBLISH channel message [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp]
func (c *Command) PublishHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetError(xerror.WrongArgsError(PUBLISH_COMMAND))
	}

	channel := args[0]

	object := NewObject(txn.UserId, PubSubDB, PubType, channel)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		id, err := uuid.NewUUID()
		if err != nil {
			return txn.SetError(err)
		}
		object.Value = id[:]
	} else if err != nil {
		return txn.SetError(err)
	}

	nowTime := txn.NowTime()
	opt, err := checkExpireOption(PUBLISH_COMMAND, args[2:], false, nowTime)
	if err != nil {
		return txn.SetError(err)
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

	count, err := c.AddCount(txn, MessageType, uint64(object.Hash), object.Value, args[1], 1)
	if err != nil {
		return txn.SetError(err)
	}
	utils.ZapLog.Debug("PUBLISH", zap.ByteString("channel", channel), zap.ByteString("message", args[1]), zap.Int64("count", count.Value))
	err = txn.Put(object.GetKeyFieldBytes(encodeMessageKey(count.Value)), encodeMessageValue(txn.Now, args[1]))
	if err != nil {
		return txn.SetError(err)
	}

	count, err = c.GetCount(txn, PubType, 0, channel, nil)
	if err != nil && err != store.KeyNotFound {
		return txn.SetError(err)
	}
	return SimpleInt(count.Value)
}

type pubSubConn struct {
	sync.RWMutex
	dconn    *redcon.DetachedConn
	channels map[string][]byte
	messages chan interface{}
	closed   chan struct{}
}

// FSUBSCRIBE offset channel [channel ...]
func (c *Command) fsubscribe(conn *redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 3 {
		conn.WriteError(xerror.WrongArgsString(FSUBSCRIBE_COMMAND))
		return
	}
	offset, err := utils.GetNonnegativeInt64(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	utils.ZapLog.Debug("fsubscribe", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	c._subscribe(conn, cmd.Args[2:], offset)
}

// SUBSCRIBE channel [channel ...]
func (c *Command) subscribe(conn *redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError(xerror.WrongArgsString(SUBSCRIBE_COMMAND))
		return
	}
	utils.ZapLog.Debug("subscribe", zap.String("remote", conn.RemoteAddr()),
		zap.ByteStrings("args", cmd.Args))
	c._subscribe(conn, cmd.Args[1:], NoOffset)
}

func (c *Command) _subscribe(conn *redcon.Conn, args [][]byte, offset int64) {
	dconn := conn.Detach()
	ps := &pubSubConn{
		dconn:    dconn,
		channels: make(map[string][]byte),
		messages: make(chan interface{}, 1000),
		closed:   make(chan struct{}),
	}
	c.subDetached(ps, args, offset)
	go ps.sendMessage()
	go c.runDetached(ps)
	go c.Pull(ps, offset)
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

func (c *Command) tryDeleteConnCount(channels []string) {
	for i := 0; i < 3; i++ {
		err := c.deleteConnCount(channels)
		if err != nil {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		return
	}
}

func (c *Command) deleteConnCount(channels []string) error {
	txn := c.client.NewTxn()
	defer txn.Rollback()
	err := txn.Begin()
	if err != nil {
		return err
	}
	for _, channel := range channels {
		_, err = c.AddCount(txn, PubType, 0, utils.S2B(channel), nil, -1)
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
	c.tryDeleteConnCount(channels)
	return nil
}

func (conn *pubSubConn) sendMessage() {
	for {
		select {
		case msg := <-conn.messages:
			conn.dconn.WriteAny(msg)
			conn.dconn.Flush()
			if msg == OK {
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
		conn.messages <- err
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
			object := NewObject(txn.UserId, PubSubDB, PubType, utils.S2B(ch))
			key := object.GetKeyBytes()
			err := getTxnObject(txn, key, object, true)
			if err == store.KeyNotFound {
			} else if err != nil {
				conn.messages <- err
				return
			} else {
				_, err = c.AddCount(txn, PubType, 0, utils.S2B(ch), nil, -1)
				if err != nil {
					conn.messages <- err
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
		conn.messages <- err
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

func (c *Command) subDetached(conn *pubSubConn, args [][]byte, offset int64) {
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		conn.messages <- err
		return
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
					conn.messages <- err
					return
				}
				object.Value = id[:]
				object.ValueTTL = DefaultTTL
				object.Timestamp = txn.Timestamp
				err = txn.Put(key, ObjectEncode(object))
			}

			if err != nil {
				conn.messages <- err
				return
			}
			_, err = c.AddCount(txn, PubType, 0, ch, nil, 1)
			if err != nil {
				conn.messages <- err
				return
			}
			var start []byte
			if offset == NoOffset {
				start = object.GetValueBytesPrefix(nil)
			} else {
				start = object.GetValueBytesPrefix(encodeMessageKey(offset))
			}

			resps[i] = func() {
				conn.Lock()
				conn.channels[cha] = start
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
		conn.messages <- err
		return
	}

	for _, resp := range resps {
		resp()
	}
}

func (c *Command) Pull(conn *pubSubConn, offset int64) {
	c.pullMessages(conn, offset)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.pullMessages(conn, offset)
		case <-conn.closed:
			return
		}
	}
}

func (c *Command) pullMessages(sconn *pubSubConn, offset int64) {
	chans := make(map[string][]byte)
	sconn.RLock()
	for c, start := range sconn.channels {
		chans[c] = start
	}
	sconn.RUnlock()
	for len(chans) > 0 {
		select {
		case <-sconn.closed:
			return
		default:
		}
		for ch, start := range chans {
			lastKey, next, err := c.pullMessgeFromChannel(sconn, ch, start, offset, 100)
			if next {
				utils.ZapLog.Debug("pull message next", zap.String("channel", ch),
					zap.ByteString("lastKey", lastKey))
				chans[ch] = lastKey
				continue
			}

			delete(chans, ch)
			if err == nil {
				continue
			}

			tick := time.NewTicker(time.Millisecond * 100)
			select {
			case <-sconn.closed:
				return
			case <-tick.C:
			}
		}
	}
}

func (c *Command) pullMessgeFromChannel(conn *pubSubConn, channel string, start []byte,
	offset int64, limit int) ([]byte, bool, error) {
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		return nil, false, err
	}
	txn.Conn = conn.dconn.Conn

	object := NewObject(txn.UserId, PubSubDB, PubType, utils.S2B(channel))
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	prefix := object.GetValueBytesPrefix(nil)
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
		if offset == NoOffset {
			conn.messages <- []interface{}{"message", channel, message}
		} else {
			conn.messages <- []interface{}{"message", channel, message, SimpleInt(int64(newOffset))}
		}
		return true
	}
	err = txn.List(start, end, limit, callback)
	if lastKey != nil {
		lastKey = utils.NextKey(lastKey)
		utils.ZapLog.Debug("pullMessgeFromChannel", zap.String("channel", channel),
			zap.Int("count", count), zap.ByteString("next", lastKey))
		conn.Lock()
		conn.channels[channel] = lastKey
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
				sconn.messages <- xerror.WrongArgsError(SUBSCRIBE_COMMAND)
				continue
			}
			c.subDetached(sconn, cmd.Args[1:], NoOffset)
		case "fsubscribe":
			if len(cmd.Args) < 3 {
				sconn.messages <- xerror.WrongArgsError(FSUBSCRIBE_COMMAND)
				continue
			}
			offset, err := utils.GetNonnegativeInt64(cmd.Args[1])
			if err != nil {
				sconn.messages <- err
				return
			}
			c.subDetached(sconn, cmd.Args[2:], offset)
		case "unsubscribe":
			c.unsubDetached(sconn, cmd.Args[1:])
		case "quit":
			sconn.messages <- OK
			return
		case "ping":
			var msg string
			switch len(cmd.Args) {
			case 1:
			case 2:
				msg = string(cmd.Args[1])
			default:
				sconn.messages <- xerror.WrongArgsError(PING_COMMAND)
				continue
			}
			sconn.messages <- []interface{}{"pong", msg}
		default:
			sconn.messages <- xerror.InvalidCommandError(comma)
		}
	}
}
