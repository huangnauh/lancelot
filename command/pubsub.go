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
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const (
	DefaultTTL       = 30 * 60 * 1000 // 30 minutes
	PubSubDB   uint8 = 200
)

func getMessageKey(count int64) []byte {
	k := make([]byte, 9)
	if count < 0 {
		k[0] = 0x00
		count = -count
	} else {
		k[0] = 0x01
	}
	binary.BigEndian.PutUint64(k, uint64(count))
	return k
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
	err = txn.Put(object.GetKeyFieldBytes(getMessageKey(count.Value)), encodeMessageValue(txn.Now, args[1]))
	if err != nil {
		return txn.SetError(err)
	}

	count, err = c.GetCount(txn, PubType, 0, channel, nil)
	if err != nil {
		return txn.SetError(err)
	}
	return SimpleInt(int(count.Value))
}

type pubSubConn struct {
	sync.RWMutex
	dconn    *redcon.DetachedConn
	channels map[string][]byte
	messages chan interface{}
	closed   chan struct{}
}

// SUBSCRIBE channel [channel ...]
func (c *Command) subscribe(conn *redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError(xerror.WrongArgsString(SUBSCRIBE_COMMAND))
		return
	}
	dconn := conn.Detach()
	ps := &pubSubConn{
		dconn:    dconn,
		channels: make(map[string][]byte),
		messages: make(chan interface{}, 1000),
		closed:   make(chan struct{}),
	}
	c.subDetached(ps, cmd.Args[1:])
	go ps.sendMessage()
	go c.runDetached(ps)
	go c.Pull(ps)
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
		conn.messages <- []interface{}{"unsubscribe", "all", 0}
	} else {
		for _, channel := range resps {
			conn.Lock()
			delete(conn.channels, channel)
			count := len(conn.channels)
			conn.Unlock()
			conn.messages <- []interface{}{"unsubscribe", channel, count}
		}
	}
}

func (c *Command) subDetached(conn *pubSubConn, args [][]byte) {
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
			start := object.GetValueBytesPrefix()
			resps[i] = func() {
				conn.Lock()
				conn.channels[cha] = start
				count := len(conn.channels)
				conn.Unlock()
				utils.ZapLog.Debug("subscribe", zap.String("channel", cha), zap.ByteString("next", start))
				conn.messages <- []interface{}{"subscribe", cha, count}
			}
		} else {
			resps[i] = func() {
				conn.RLock()
				count := len(conn.channels)
				conn.RUnlock()
				conn.messages <- []interface{}{"subscribe", cha, count}
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

func (c *Command) Pull(conn *pubSubConn) {
	c.pullMessages(conn)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.pullMessages(conn)
		case <-conn.closed:
			return
		}
	}
}

func (c *Command) pullMessages(sconn *pubSubConn) {
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
			lastKey, next, err := c.pullMessgeFromChannel(sconn, ch, start, 100)
			if next {
				utils.ZapLog.Debug("pull message next", zap.String("channel", ch), zap.ByteString("lastKey", lastKey))
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

func (c *Command) pullMessgeFromChannel(conn *pubSubConn, channel string, start []byte, limit int) ([]byte, bool, error) {
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
	end := utils.PrefixNext(object.GetValueBytesPrefix())
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
		if math.Abs(float64(timestamp-txn.Now)) < 1000 {
			next = true
		}
		conn.messages <- []interface{}{"message", channel, message}
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
			c.subDetached(sconn, cmd.Args[1:])
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
