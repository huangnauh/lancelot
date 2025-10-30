package store

import (
	"context"
	"strconv"
	"time"

	"github.com/huangnauh/lancelot/utils"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	gcDefaultLifeTime    = time.Minute * 10
	gcWorkerTickInterval = time.Minute
	GcSafePoint          = "/lancelot/gcworker/saved_tikv_safe_point"
)

func (c *Client) RunGC() {
	if len(c.conf.PDAddrs) == 0 {
		return
	}
	c.gcTick()
	tick := time.NewTicker(gcWorkerTickInterval)
	for {
		select {
		case <-tick.C:
			c.gcTick()
		case <-c.ctx.Done():
			tick.Stop()
			return
		}
	}
}

func (c *Client) gcTick() {
	ctx, cancel := context.WithTimeout(c.ctx, time.Second*10)
	defer cancel()
	leader := c.manager.GetLeader(ctx)
	if leader != c.uuid {
		utils.ZapLog.Info("not leader, skip gc", zap.String("leader", leader), zap.String("uuid", c.uuid))
		return
	}

	safePoint, safePointValue, err := c.getNewSafePoint()
	if err != nil {
		return
	}

	lastSafePoint, err := c.getSafePoint(GcSafePoint)
	if err != nil {
		utils.ZapLog.Error("get safepoint", zap.String("gc-saved", GcSafePoint))
		return
	}

	if lastSafePoint.Add(gcWorkerTickInterval).After(safePoint) {
		utils.ZapLog.Info("safe point is not expired", zap.Time("lastSafePoint", lastSafePoint), zap.Time("safePoint", safePoint))
		return
	}

	err = c.saveSafePoint(GcSafePoint, safePointValue)
	if err != nil {
		utils.ZapLog.Error("get safepoint", zap.String("gc-saved", GcSafePoint))
		return
	}

	utils.ZapLog.Info("gc start", zap.Time("safePoint", safePoint), zap.String("id", c.uuid))
	_, err = c.store.GC(context.Background(), safePointValue)
	if err != nil {
		return
	}
	utils.ZapLog.Info("gc finished", zap.Time("safePoint", safePoint), zap.String("id", c.uuid))
}

func (c *Client) getNewSafePoint() (time.Time, uint64, error) {
	currentVer, err := c.CurrentVersion()
	if err != nil {
		return time.Time{}, 0, err
	}
	physical := oracle.ExtractPhysical(currentVer)
	sec, nsec := physical/1e3, (physical%1e3)*1e6
	now := time.Unix(sec, nsec)
	safePoint := now.Add(-gcDefaultLifeTime)
	safePointValue := oracle.ComposeTS(oracle.GetPhysical(safePoint), 0)
	return safePoint, safePointValue, nil
}

func (c *Client) getSafePoint(key string) (time.Time, error) {
	value, err := c.GetSafePointKV().Get(key)
	if err != nil {
		utils.ZapLog.Error("get safe point failed", zap.Error(err))
		return time.Time{}, err
	}

	safePointTS, err := strconv.ParseUint(value, 10, 64)
	safePoint := time.Unix(0, oracle.ExtractPhysical(safePointTS)*1e6)
	return safePoint, nil
}

func (c *Client) saveSafePoint(key string, safePointValue uint64) error {
	s := strconv.FormatUint(safePointValue, 10)
	err := c.GetSafePointKV().Put(key, s)
	if err != nil {
		utils.ZapLog.Error("get safe point failed", zap.Uint64("safepoint", safePointValue), zap.Error(err))
		return err
	}
	return nil
}
