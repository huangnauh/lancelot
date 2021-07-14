package server

import (
	"bytes"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/lancelot/command"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
)

const (
	GcSavedTs = "/lancelot/gcworker/saved_ts"
)

func (s *Server) StartGC() {
	ticker := time.NewTicker(s.cfg.GcTickInterval)
	defer func() {
		ticker.Stop()
		close(s.gcClosed)
	}()
	for {
		select {
		case <-ticker.C:
			s.tickGC()
		case <-s.closed:
			return
		}
	}
}

func (s *Server) tickGC() {
	ts, err := s.client.CurrentVersion()
	if err != nil {
		logrus.Errorf("get current version err: %s", err)
		return
	}

	ms := oracle.ExtractPhysical(ts)
	now := time.Unix(ms/1e3, (ms%1e3)*1e6)
	loadTS, err := s.client.LoadTS(GcSavedTs)
	if err != nil {
		logrus.Errorf("load ts err: %s", err)
		return
	}

	saved := oracle.GetTimeFromTS(loadTS)
	if saved.Add(time.Minute).After(now) {
		return
	}
	err = s.client.SaveTS(GcSavedTs, ts)
	if err != nil {
		logrus.Errorf("save ts err: %s", err)
		return
	}

	cur := command.GetTTLPrefix(0)
	endGC := command.GetTTLPrefix(ms)
	touchTicker := time.NewTicker(s.cfg.GcTickInterval / 10)
	defer touchTicker.Stop()

LABLE:
	for bytes.Compare(cur, endGC) < 0 {
		var limit int
		var wait bool
		if atomic.LoadInt32(&s.gcWorkers) < int32(s.cfg.GcWorkers) {
			cur, limit, wait, err = s.doGC(cur, endGC)
			if err != nil {
				break LABLE
			}
			if limit < s.cfg.Store.BatchLimit {
				break LABLE
			}

			select {
			case <-s.closed:
				break LABLE
			default:
				if !wait {
					continue
				}
			}
		}

		select {
		case <-s.closed:
			break LABLE
		case <-touchTicker.C:
			ts, err := s.client.CurrentVersion()
			if err != nil {
				break LABLE
			}
			err = s.client.SaveTS(GcSavedTs, ts)
			if err != nil {
				break LABLE
			}
		}
	}
	s.gcWait.Wait()
}

func (s *Server) DelteRange(start, end []byte, callback func(*store.Client)) {
	s.client.DelteRange(start, end, callback)
	s.gcWait.Done()
	atomic.AddInt32(&s.gcWorkers, -1)
}

func (s *Server) doGC(start, end []byte) ([]byte, int, bool, error) {
	logrus.Infof("start gc %v %v", start, end)
	txn := s.client.NewTxn()
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
		object, err := command.GetObjectFromTTL(k)
		if err != nil {
			logrus.Errorf("get key %s from ttl err: %s", k, err)
			continue
		}
		switch object.Type {
		case command.KeyType:
			key := command.GetKeyBytes(object.Type, object.Key)
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
			if count >= s.cfg.Store.BatchLimit {
				break LABLE
			}
		case command.HashType:
			p := object.GetKeyBytesPrefix()
			logrus.Debugf("delete hash: %s", p)
			s.gcWait.Add(1)
			gcWorkers := atomic.AddInt32(&s.gcWorkers, 1)
			go s.DelteRange(p, utils.PrefixNext(p), func(c *store.Client) {
				_ = c.Delete(k)
			})
			if int(gcWorkers) >= s.cfg.GcWorkers {
				// limit the number of goroutines
				wait = true
				count = s.cfg.Store.BatchLimit
				break LABLE
			}
		default:
			logrus.Errorf("not support object: %s", object.ObjectEncoding())
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
