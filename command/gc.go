package command

import (
	"bytes"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
)

const (
	GcSavedTs = "/lancelot/gcworker/saved_ts"
)

func (c *Command) startGC() {
	ticker := time.NewTicker(c.cfg.GcTickInterval)
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
		logrus.Errorf("get current version err: %s", err)
		return
	}

	ms := oracle.ExtractPhysical(ts)
	now := time.Unix(ms/1e3, (ms%1e3)*1e6)
	loadTS, err := c.client.LoadTS(GcSavedTs)
	if err != nil {
		logrus.Errorf("load ts err: %s", err)
		return
	}

	saved := oracle.GetTimeFromTS(loadTS)
	if saved.Add(c.cfg.GcTickInterval).After(now) {
		return
	}
	err = c.client.SaveTS(GcSavedTs, ts)
	if err != nil {
		logrus.Errorf("save ts err: %s", err)
		return
	}

	cur := GetTTLPrefix(0)
	endGC := GetTTLPrefix(ms)
	touchTicker := time.NewTicker(c.cfg.GcTickInterval / 10)
	defer touchTicker.Stop()

LABLE:
	for bytes.Compare(cur, endGC) < 0 {
		var limit int
		var wait bool
		if atomic.LoadInt32(&c.gcWorkers) < int32(c.cfg.GcWorkers) {
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
	logrus.Infof("start gc %v %v", start, end)
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		logrus.Errorf("new txn err: %s", err)
		return nil, 0, false, err
	}
	defer txn.Rollback()
	it, err := txn.Iter(start, end, false)
	if err != nil {
		logrus.Errorf("iter err: %s", err)
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
			logrus.Errorf("get key %s from ttl err: %s", k, err)
			continue
		}
		if len(object.Key) > 0 {
			key := object.GetKeyBytes()
			logrus.Debugf("delete key: %s", key)
			err = txn.Del(key)
			if err != nil {
				logrus.Errorf("del key %s err: %s", key, err)
			}
			count++
			err = txn.Del(k)
			if err != nil {
				logrus.Errorf("del key %s err: %s", k, err)
			}
			count++
			if count >= c.cfg.Store.BatchLimit {
				break LABLE
			}
		}

		if len(object.Value) > 0 {
			p := object.GetValueBytesPrefix()
			logrus.Debugf("delete hash: %s", p)
			c.gcWait.Add(1)
			gcWorkers := atomic.AddInt32(&c.gcWorkers, 1)
			go c.DelteRange(p, utils.PrefixNext(p), func(c *store.Client) {
				_ = c.Delete(k)
			})
			if int(gcWorkers) >= c.cfg.GcWorkers {
				// limit the number of goroutines
				wait = true
				count = c.cfg.Store.BatchLimit
				break LABLE
			}
		}

		err = it.Next()
		if err != nil {
			logrus.Errorf("next err: %s", err)
			return k, 0, false, err
		}
	}
	err = txn.Commit()
	if err != nil {
		logrus.Errorf("commit err: %s", err)
	}
	return k, count, wait, err
}
