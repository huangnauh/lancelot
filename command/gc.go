package command

import (
	"bytes"
	"context"
	"sync/atomic"
	"time"

	"github.com/tikv/client-go/v2/oracle"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
)

const (
	GcSavedTs = "/lancelot/gcworker/saved_command_safe_point"
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

func (c *Command) touchGC() {
	touchTicker := time.NewTicker(c.cfg.GC.TickInterval / 10)
	defer touchTicker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-c.gcClosed:
			return
		case <-touchTicker.C:
			ts, err := c.client.CurrentVersion()
			if err != nil {
				continue
			}
			err = c.client.SaveTS(GcSavedTs, ts)
			if err != nil {
				continue
			}
		}
	}
}

func (c *Command) tickGC() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	leader := c.client.GetLeader(ctx)
	if leader != c.client.ID() {
		utils.ZapLog.Debug("[gc] not leader, skip gc", zap.String("leader", leader), zap.String("id", c.client.ID()))
		return
	}

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
		utils.ZapLog.Debug("[gc] not reach tick interval", zap.Time("saved", saved), zap.Time("now", now))
		return
	}
	err = c.client.SaveTS(GcSavedTs, ts)
	if err != nil {
		utils.ZapLog.Error("[gc] save ts", zap.Error(err))
		return
	}

	go c.touchGC()

	// go c.gcPubSub(ms)
	utils.ZapLog.Debug("[gc] start gc", zap.Time("now", now))
	cur := []byte{byte(TTLPrefix)}
	endGC := []byte{byte(TTLPrefix + 1)}
LABLE:
	for bytes.Compare(cur, endGC) < 0 {
		select {
		case <-c.done:
			break LABLE
		default:
		}

		if atomic.LoadInt32(&c.gcWorkers) < int32(c.cfg.GC.TTLWorkers) {
			lastKey, o, err := c.doGC(cur, endGC, ms)
			if err != nil {
				break LABLE
			}
			if lastKey == nil && o == nil {
				break LABLE
			}

			if lastKey == nil {
				cur = GetUserDBPrefix(TTLPrefix, o.UserId, o.Db+1)
			} else {
				cur = lastKey
			}
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
	c.gcWait.Wait()
	c.gcClosed <- true
	endTime := time.Now()
	utils.ZapLog.Debug("[gc] end gc", zap.Time("now", endTime))
}

func (c *Command) DelteRange(start, end []byte, callback func(*store.Client)) {
	c.client.DelteRange(start, end, callback)
	c.gcWait.Done()
	atomic.AddInt32(&c.gcWorkers, -1)
}

func (c *Command) doGC(start, end []byte, now int64) ([]byte, *Object, error) {
	utils.ZapLog.Debug("[gc] start gc", zap.ByteString("start", start), zap.ByteString("end", end))
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		return nil, nil, err
	}
	defer txn.Rollback()
	it, err := txn.Iter(start, end, false)
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()
	count := 0
	var lastKey []byte
	var o *Object
	finish := false
	for it.Valid() {
		lastKey = it.Key()
		if bytes.Compare(lastKey, end) >= 0 {
			finish = true
			break
		}

		lastValue := it.Value()
		object, err := GetObjectFromTTL(lastKey, lastValue)
		if err != nil {
			utils.ZapLog.Warn("invalid key", zap.ByteString("value", lastValue), zap.ByteString("key", lastKey), zap.Error(err))
			err = txn.Del(lastKey)
			if err != nil {
				utils.ZapLog.Error("[gc] del invalid ttl key", zap.ByteString("ttl key", lastKey), zap.Error(err))
			}
			err = it.Next()
			if err != nil {
				utils.ZapLog.Error("[gc] iter next", zap.Error(err))
				return lastKey, nil, err
			}
			continue
		}

		o = object
		if object.TTL >= now {
			lastKey = nil
			utils.ZapLog.Debug("[gc] object not expired", zap.Int64("ttl", object.TTL),
				zap.ByteString("ttl key", lastKey), zap.ByteString("key", object.Key))
			break
		}
		if len(object.Value) > 0 {
			p := object.GetValueBytes(nil)
			utils.ZapLog.Debug("[gc] delete value", zap.ByteString("value", object.Value), zap.ByteString("key", object.Key))
			c.gcWait.Add(1)
			gcWorkers := atomic.AddInt32(&c.gcWorkers, 1)
			ttlKey := lastKey
			go c.DelteRange(p, utils.PrefixNext(p), func(_ *store.Client) {
				txn := c.client.NewTxn()
				err = txn.Begin()
				if err != nil {
					return
				}
				defer txn.Rollback()
				if len(object.Key) == 0 {
					err = c.CleanKey(txn, nil, ttlKey, object, 0, MinusCount|GCChange)
				} else {
					err = c.CleanKey(txn, object.GetKeyBytes(), ttlKey, object, 0, MinusCount|GCChange)
				}
				if err != nil {
					utils.ZapLog.Error("[gc] del key", zap.ByteString("value", object.Value), zap.ByteString("key", object.Key), zap.Error(err))
					return
				}
				txn.Commit()
			})
			if int(gcWorkers) >= c.cfg.GC.TTLWorkers {
				// limit the number of goroutines
				break
			}
		} else if len(object.Key) > 0 {
			count++
			utils.ZapLog.Debug("[gc] del key", zap.ByteString("ttl key", lastKey), zap.ByteString("key", object.Key))
			err = c.CleanKey(txn, object.GetKeyBytes(), lastKey, object, 0, MinusCount|GCChange)
			if err != nil {
				utils.ZapLog.Error("[gc] del key", zap.ByteString("ttl key", lastKey), zap.Error(err))
				return lastKey, nil, err
			}
			if count >= c.cfg.Store.BatchLimit {
				break
			}
		}

		err = it.Next()
		if err != nil {
			utils.ZapLog.Error("[gc] iter next", zap.Error(err))
			return lastKey, nil, err
		}
	}
	err = txn.Commit()
	if err != nil {
		utils.ZapLog.Error("[gc] commit", zap.Error(err))
		return lastKey, nil, err
	}
	utils.ZapLog.Debug("[gc] end gc", zap.Bool("finish", finish), zap.ByteString("last key", lastKey),
		zap.Any("object", o), zap.ByteString("start", start), zap.ByteString("end", end))

	if finish || !it.Valid() {
		return nil, nil, nil
	}

	if lastKey == nil {
		return nil, o, nil
	}
	return utils.NextKey(lastKey), o, nil
}
