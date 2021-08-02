package command

import (
	"encoding/binary"
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

func getMessageKey(value []byte, txn *store.Txn) []byte {
	id := txn.GetCurrentID()
	k := make([]byte, len(value)+8+4)
	copy(k[:len(value)], value)
	binary.BigEndian.PutUint64(k[len(value):], uint64(txn.Timestamp))
	binary.BigEndian.PutUint32(k[len(value)+8:], id)
	return k
}

func getMessagePrefix(value []byte, timestamp uint64) []byte {
	k := make([]byte, len(value)+8)
	copy(k[:len(value)], value)
	binary.BigEndian.PutUint64(k[len(value):], uint64(timestamp))
	return k
}

// (pubsub) PUBLISH channel message [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp]
func (c *Command) PublishHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetError(xerror.WrongArgsError(PUBLISH_COMMAND))
	}

	channel := args[0]

	object := NewObject(txn.UserId, txn.DBId, PubType, channel)
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

	oldTTL := object.TTL
	if opt.Expire > 0 {
		object.TTL = opt.Expire
	} else if object.TTL == 0 {
		object.TTL = 60 * 30 * 1000 // 30 minutes
	}

	if object.TTL != oldTTL {
		object.Timestamp = txn.Timestamp
		err = txn.Put(key, ObjectEncode(object))
		if err != nil {
			return txn.SetError(err)
		}
	}

	message := NewObject(txn.UserId, txn.DBId, MessageType, getMessageKey(object.Value, txn))
	message.Value = args[1]
	message.Timestamp = txn.Timestamp
	err = txn.Put(message.GetKeyBytes(), ObjectEncode(message))
	if err != nil {
		return txn.SetError(err)
	}
	return SimpleInt(object.Count)
}

type pubSubConn struct {
	sync.RWMutex
	dconn    *redcon.DetachedConn
	channels map[string][]byte
	messages chan interface{}
}

// SUBSCRIBE channel [channel ...]
func (c *Command) subscribe(conn *redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError(xerror.WrongArgsString(SUBSCRIBE_COMMAND))
		return
	}
	dconn := conn.Detach()
	ps := &pubSubConn{dconn: dconn, channels: make(map[string][]byte), messages: make(chan interface{}, 1000)}
	c.subDetached(ps, cmd.Args[1:])
	go c.runDetached(ps)
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

type subCount struct {
	channel string
	start   []byte
}

func (conn *pubSubConn) Close() {
	conn.dconn.Close()
}

func (conn *pubSubConn) sendMessage() {
	for {
		select {
		case msg := <-conn.messages:
			conn.dconn.WriteAny(msg)
			conn.dconn.Flush()
			if msg == OK {
				conn.Close()
				return
			}
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
	for i, c := range channels {
		if conn.isChannelExist(c) && !argsCh[c] {
			argsCh[c] = true
			object := NewObject(txn.UserId, txn.DBId, PubType, utils.S2B(c))
			key := object.GetKeyBytes()
			err := getTxnObject(txn, key, object, true)
			if err == store.KeyNotFound {
			} else if err != nil {
				conn.messages <- err
				return
			} else {
				if object.Count > 0 {
					object.Count--
				}
				object.Timestamp = txn.Timestamp
				err = txn.Put(key, ObjectEncode(object))
				if err != nil {
					conn.messages <- err
					return
				}
			}
		}
		if !all {
			resps[i] = c
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
	for _, ch := range args {
		c := string(ch)
		if !conn.isChannelExist(c) && !argsCh[c] {
			argsCh[c] = true
			object := NewObject(txn.UserId, txn.DBId, PubType, ch)
			key := object.GetKeyBytes()
			err := getTxnObject(txn, key, object, true)
			if err == store.KeyNotFound {
				id, err := uuid.NewUUID()
				if err != nil {
					conn.messages <- err
					return
				}
				object.Value = id[:]
			} else if err != nil {
				conn.messages <- err
				return
			}
			object.Count++
			object.Timestamp = txn.Timestamp
			err = txn.Put(key, ObjectEncode(object))
			if err != nil {
				conn.messages <- err
				return
			}
			resps = append(resps, func() {
				conn.Lock()
				conn.channels[c] = getMessageKeyPrefix(txn, object.Value, txn.Timestamp)
				count := len(conn.channels)
				conn.Unlock()
				conn.messages <- []interface{}{"subscribe", c, count}
			})
		} else {
			resps = append(resps, func() {
				conn.RLock()
				count := len(conn.channels)
				conn.RUnlock()
				conn.messages <- []interface{}{"subscribe", c, count}
			})
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

func (c *Command) pullMessage(sconn *pubSubConn) {
	chans := make(map[string][]byte, 0)
	sconn.RLock()
	for c, start := range sconn.channels {
		chans[c] = start
	}
	sconn.RUnlock()
	for ch, start := range chans {
		c.pullMessgeFromChannel(sconn, ch, start, 100)
	}
}

func getMessageKeyPrefix(txn *store.Txn, value []byte, timestamp uint64) []byte {
	return GetDataPrefix(txn.UserId, txn.DBId, MessageType, getMessagePrefix(value, timestamp))
}

func (c *Command) pullMessgeFromChannel(conn *pubSubConn, channel string, start []byte, limit int) error {
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		return err
	}
	txn.Conn = conn.dconn.Conn

	object := NewObject(txn.UserId, txn.DBId, PubType, utils.S2B(channel))
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return err
	}
	// start := getMessageKeyPrefix(txn, object.Value, timestamp)
	end := getMessageKeyPrefix(txn, object.Value, txn.Timestamp)
	var lastKey []byte
	callback := func(key, value []byte) bool {
		lastKey = key
		object, err := GetObjectFromKV(key, value)
		if err != nil {
			utils.ZapLog.Error("scan object", zap.String("remote", txn.RemoteAddr()),
				zap.Uint64("timestamp", txn.Timestamp), zap.Binary("key", key), zap.Binary("value", value), zap.Error(err))
			return true
		}
		conn.messages <- []interface{}{"message", channel, object.Value}
		return true
	}
	err = txn.List(start, end, limit, callback)
	if err != nil {
		return err
	}
	if lastKey != nil {
		conn.Lock()
		conn.channels[channel] = lastKey
		conn.Unlock()
	}
	return nil
}

func (c *Command) runDetached(sconn *pubSubConn) {
	ticker := time.NewTicker(c.cfg.GcTickInterval)
	defer func() {
		ticker.Stop()
	}()
	for {
		cmd, err := sconn.dconn.ReadCommand()
		if err != nil {
			return
		}
		if len(cmd.Args) == 0 {
			continue
		}
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
