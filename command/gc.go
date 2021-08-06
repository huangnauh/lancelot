package command

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
)

const (
	GcSavedTs = "/lancelot/gcworker/saved_ts"
)

func (c *Command) startGC() {
	ticker := time.NewTicker(c.cfg.GC.TickInterval)
	defer func() {
		ticker.Stop()
		close(c.gcClosed)
	}()
	for {
		select {
		case <-ticker.C:
			c.tickGC()
		case <-c.done:
			return
		}
	}
}

func (c *Command) tickGC() {
	ts, err := c.client.CurrentVersion()
	if err != nil {
		utils.ZapLog.Error("[gc] get current version", zap.Error(err))
		return
	}

	ms := oracle.ExtractPhysical(ts)
	now := time.Unix(ms/1e3, (ms%1e3)*1e6)
	loadTS, err := c.client.LoadTS(GcSavedTs)
	if err != nil {
		utils.ZapLog.Error("[gc] load ts", zap.Error(err))
		return
	}

	saved := oracle.GetTimeFromTS(loadTS)
	if saved.Add(c.cfg.GC.TickInterval).After(now) {
		return
	}
	err = c.client.SaveTS(GcSavedTs, ts)
	if err != nil {
		utils.ZapLog.Error("[gc] save ts", zap.Error(err))
		return
	}

	go c.gcPubSub(ms)

	cur := GetTTLPrefix(0)
	endGC := GetTTLPrefix(ms)
	touchTicker := time.NewTicker(c.cfg.GC.TickInterval / 10)
	defer touchTicker.Stop()

LABLE:
	for bytes.Compare(cur, endGC) < 0 {
		var limit int
		var wait bool
		if atomic.LoadInt32(&c.gcWorkers) < int32(c.cfg.GC.TTLWorkers) {
			cur, limit, wait, err = c.doGC(cur, endGC)
			if err != nil {
				break LABLE
			}
			if limit < c.cfg.Store.BatchLimit {
				break LABLE
			}

			select {
			case <-c.done:
				break LABLE
			default:
				if !wait {
					continue
				}
			}
		}

		select {
		case <-c.done:
			break LABLE
		case <-touchTicker.C:
			ts, err := c.client.CurrentVersion()
			if err != nil {
				break LABLE
			}
			err = c.client.SaveTS(GcSavedTs, ts)
			if err != nil {
				break LABLE
			}
		}
	}
	c.gcWait.Wait()
}

func (c *Command) DelteRange(start, end []byte, callback func(*store.Client)) {
	c.client.DelteRange(start, end, callback)
	c.gcWait.Done()
	atomic.AddInt32(&c.gcWorkers, -1)
}

func (c *Command) doGC(start, end []byte) ([]byte, int, bool, error) {
	utils.ZapLog.Info("[gc] start gc", zap.Binary("start", start), zap.Binary("end", end))
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		utils.ZapLog.Error("[gc] new txn", zap.Error(err))
		return nil, 0, false, err
	}
	defer txn.Rollback()
	it, err := txn.Iter(start, end, false)
	if err != nil {
		utils.ZapLog.Error("[gc] iter", zap.Error(err))
		return nil, 0, false, err
	}
	defer it.Close()
	count := 0
	k := start
	wait := false
LABLE:
	for it.Valid() && bytes.Compare(k, end) < 0 && bytes.Compare(k, start) >= 0 {
		k = it.Key()
		object, err := GetObjectFromTTL(k)
		if err != nil {
			utils.ZapLog.Error("[gc] get object from ttl", zap.Binary("key", k), zap.Error(err))
			continue
		}
		if len(object.Key) > 0 {
			key := object.GetKeyBytes()
			utils.ZapLog.Debug("[gc] delete key", zap.ByteString("key", object.Key), zap.Binary("keybytes", key))
			err = txn.Del(key)
			if err != nil {
				utils.ZapLog.Error("[gc] del key", zap.ByteString("key", object.Key), zap.Binary("keybytes", key), zap.Error(err))
			}
			count++
			err = txn.Del(k)
			if err != nil {
				utils.ZapLog.Error("[gc] del key", zap.ByteString("key", object.Key), zap.Binary("keybytes", key), zap.Error(err))
			}
			count++
			if count >= c.cfg.Store.BatchLimit {
				break LABLE
			}
		}

		if len(object.Value) > 0 && object.Type == HashType {
			p := object.GetValueBytesPrefix()
			utils.ZapLog.Debug("[gc] delete hash", zap.ByteString("value", object.Value), zap.Binary("prefix", p))
			c.gcWait.Add(1)
			gcWorkers := atomic.AddInt32(&c.gcWorkers, 1)
			ttlKey := k
			go c.DelteRange(p, utils.PrefixNext(p), func(c *store.Client) {
				_ = c.Delete(ttlKey)
			})
			if int(gcWorkers) >= c.cfg.GC.TTLWorkers {
				// limit the number of goroutines
				wait = true
				count = c.cfg.Store.BatchLimit
				break LABLE
			}
		}

		err = it.Next()
		if err != nil {
			utils.ZapLog.Error("[gc] iter next", zap.Error(err))
			return k, 0, false, err
		}
	}
	err = txn.Commit()
	if err != nil {
		utils.ZapLog.Error("[gc] commit", zap.Error(err))
	}
	return k, count, wait, err
}

type messgeGC struct {
	start  []byte
	end    []byte
	expire int64
	hash   uint16
	key    []byte
}

func (c *Command) TryDeleteChannel(userID uint16, channel string, hash uint16, key []byte) {
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		return
	}
	defer txn.Rollback()

	var expire time.Time
	count, err := c.GetCount(txn, PubType, 0, utils.S2B(channel), nil)
	if err != nil {
		return
	}
	if count.Value > 0 {
		return
	}

	now := txn.NowTime()
	timestamp := oracle.ExtractPhysical(count.Timestamp)
	exist := time.Unix(timestamp/1e3, (timestamp%1e3)*1e6)
	if now.Sub(exist) < c.cfg.GC.PubChannelExpire {
		utils.ZapLog.Debug("[gc] not expire", zap.String("channel", channel),
			zap.Time("exist", exist), zap.Time("now", now), zap.Int64("count", count.Value))
		return
	}
	utils.ZapLog.Debug("[gc] channel expired", zap.String("channel", channel),
		zap.Time("exist", exist), zap.Time("now", now), zap.Int64("count", count.Value))

	err = c.DeleteCount(txn, PubType, 0, utils.S2B(channel), time.Time{})
	if err != nil {
		return
	}

	object := NewObject(userID, PubSubDB, PubType, utils.S2B(channel))
	k := object.GetKeyBytes()
	err = txn.Del(k)
	if err != nil {
		return
	}

	expire = now.Add(-c.cfg.GC.PubChannelExpire)
	err = c.DeleteCount(txn, MessageType, hash, key, expire)
	if err != nil {
		return
	}

	utils.ZapLog.Info("[gc] delete channel count", zap.String("channel", channel), zap.Uint16("hash", hash), zap.Binary("key", key))
	_ = txn.Commit()
}

func (c *Command) DeleteChannelMessage(userID uint16, name string, channel messgeGC) {
	count := 0
	_ = c.client.DeleteRangeUntil(channel.start, channel.end, func(k, v []byte) bool {
		count++
		if len(v) < 8 {
			return true
		}
		t := binary.BigEndian.Uint64(v[:8])
		return t < uint64(channel.expire)
	})
	if count == 0 {
		c.TryDeleteChannel(userID, name, channel.hash, channel.key)
	}
	utils.ZapLog.Info("[gc] delete channel", zap.String("channel", name), zap.Int("count", count), zap.Int64("expire", channel.expire))
}

func (c *Command) DeleteUserPubSub(userID uint16, now int64, limit chan struct{}) {
	utils.ZapLog.Debug("[gc] delete user pubsub", zap.Uint16("user", userID), zap.Int64("now", now))
	userStart := GetDataPrefix(userID, PubSubDB, KeyType, nil)
	userEnd := GetDataPrefix(userID, PubSubDB, KeyType, []byte{0xff})
	for {
		channels := make(map[string]messgeGC)
		var lastKey []byte
		err := c.client.List(userStart, userEnd, 100, func(k, v []byte) bool {
			lastKey = k
			object, err := GetObjectFromKV(k, v)
			if err != nil {
				return true
			}
			if object.Type != PubType {
				return true
			}
			channel := string(object.Key)
			expire := now - object.ValueTTL
			start := object.GetValueBytesPrefix()
			end := utils.PrefixNext(object.GetValueBytesPrefix())
			channels[channel] = messgeGC{start, end, expire, object.Hash, object.Value}
			return true
		})

		for name, channel := range channels {
			limit <- struct{}{}
			c.gcWait.Add(1)
			utils.ZapLog.Debug("[gc] delete user pubsub", zap.Uint16("user", userID), zap.Int64("now", now),
				zap.String("channel", name), zap.Int64("expire", channel.expire))
			go func(name string, messge messgeGC) {
				c.DeleteChannelMessage(userID, name, messge)
				<-limit
				c.gcWait.Done()
			}(name, channel)
		}

		if err == store.ReachLimit {
			userStart = utils.NextKey(lastKey)
			continue
		}
		break
	}
}

func (c *Command) gcPubSub(now int64) {
	c.gcWait.Add(1)
	defer c.gcWait.Done()

	limit := make(chan struct{}, c.cfg.GC.PubSubWorkers)
	for _, user := range c.users {
		userID := user.ID
		c.DeleteUserPubSub(userID, now, limit)
	}
}
