package command

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/axiomhq/hyperloglog"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
)

const (
	HLLNoAccess = 10 * time.Minute
)

type hll struct {
	sync.RWMutex
	Key        string
	Sketch     *hyperloglog.Sketch
	AccessTime time.Time
	UpdateTime time.Time
	FlushTime  time.Time
}

type Hll struct {
	sync.RWMutex
	Keys map[string]*hll
	wg   *sync.WaitGroup
}

var (
	HLogLog = NewHll()
)

func GetHllKey(userId uint16, dbId uint8, key []byte) string {
	return fmt.Sprintf("%d-%d-%s", userId, dbId, utils.B2S(key))
}

func GetUserAndKey(key string) (uint16, uint8, []byte, error) {
	s := strings.SplitN(key, "-", 3)
	if len(s) != 3 {
		return 0, 0, nil, fmt.Errorf("invalid key %s", key)
	}
	userId, err := strconv.Atoi(s[0])
	if err != nil {
		return 0, 0, nil, err
	}
	dbId, err := strconv.Atoi(s[1])
	if err != nil {
		return 0, 0, nil, err
	}
	return uint16(userId), uint8(dbId), []byte(s[2]), nil
}

func NewHll() *Hll {
	return &Hll{
		Keys: make(map[string]*hll),
		wg:   &sync.WaitGroup{},
	}
}

func (h *Hll) Get(key string) *hll {
	h.RLock()
	defer h.RUnlock()
	if v, ok := h.Keys[key]; ok {
		return v
	}
	return nil
}

func (h *Hll) Set(key string, v *hll) {
	h.Lock()
	defer h.Unlock()
	h.Keys[key] = v
}

func (h *Hll) GetALL() map[string]*hll {
	h.RLock()
	defer h.RUnlock()
	result := make(map[string]*hll, len(h.Keys))
	for k, v := range h.Keys {
		result[k] = v
	}
	return result
}

func (h *Hll) Remove(key string, nocheck bool) {
	h.Lock()
	defer h.Unlock()
	if v, ok := h.Keys[key]; ok {
		if nocheck || time.Since(v.AccessTime) > HLLNoAccess {
			delete(h.Keys, key)
		}
	}
}

func (c *Command) FlushHll(h *hll) {
	h.Lock()
	defer h.Unlock()
	if h.FlushTime.Sub(h.UpdateTime) > 0 {
		return
	}
	data, err := h.Sketch.MarshalBinary()
	if err != nil {
		utils.ZapLog.Error("Hll MarshalBinary error", zap.Error(err))
	}
	go c.FlushHllData(h, data)
}

func (c *Command) FlushHllData(h *hll, hlldata []byte) {
	HLogLog.wg.Add(1)
	defer HLogLog.wg.Done()
	utils.ZapLog.Debug("FlushHllData", zap.String("key", h.Key), zap.Int("size", len(hlldata)))
	txn := c.client.NewTxn()
	err := txn.Begin()
	if err != nil {
		return
	}
	defer txn.Rollback()
	userID, dbID, k, err := GetUserAndKey(h.Key)
	if err != nil {
		utils.ZapLog.Error("GetUserAndKey error", zap.Error(err))
		return
	}
	cfg := c.GetConfig(userID)
	txn.Config = cfg
	txn.Conn = &redcon.Conn{
		DBId:   dbID,
		UserId: userID,
	}
	var create ChangeType
	object := NewObject(txn, HLLType, k)
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, true)
	if err == store.KeyNotFound {
		create = PlusCount
	} else if err != nil {
		return
	}
	object.Value = hlldata
	object.Timestamp = txn.Timestamp
	err = setTxnObject(txn, key, object, create)
	if err != nil {
		return
	}
	err = txn.Commit()
	if err != nil {
		return
	}
	h.FlushTime = time.Now()
}

func (c *Command) FlushAllHll() {
	now := time.Now()
	all := HLogLog.GetALL()
	for k, v := range all {
		if v.UpdateTime.Sub(v.FlushTime) > 0 {
			c.FlushHll(v)
		}
		if now.Sub(v.AccessTime) > HLLNoAccess {
			HLogLog.Remove(k, false)
		}
	}
	HLogLog.wg.Wait()
}

func (c *Command) StartHll() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			c.FlushAllHll()
		case <-c.done:
			return
		}
	}
}

func (c *Command) CloseHll() {
	c.FlushAllHll()
}

func (c *Command) LoadHll(txn *store.Txn, k []byte) (*hll, error) {
	object := NewObject(txn, HLLType, k)
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, true)
	h := &hll{
		Sketch:     hyperloglog.NewNoSparse(),
		AccessTime: txn.NowTime(),
	}
	if err == store.KeyNotFound {
	} else if err != nil {
		return nil, err
	} else {
		utils.ZapLog.Debug("LoadHll", zap.String("key", string(k)), zap.Int("size", len(object.Value)))
		err = h.Sketch.UnmarshalBinary(object.Value)
		if err != nil {
			return nil, err
		}
	}
	return h, nil
}

// PFADD key [element [element ...]]
func (c *Command) PfAddHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(PFADD_COMMAND)
	}
	k := args[0]
	hk := GetHllKey(txn.UserId, txn.DBId, k)
	now := txn.NowTime()
	h := HLogLog.Get(hk)
	if h == nil {
		var err error
		h, err = c.LoadHll(txn, k)
		if err != nil {
			return txn.SetError(err)
		}
		h.Key = hk
		HLogLog.Set(hk, h)
	}
	h.AccessTime = now
	count := 0
	for _, v := range args[1:] {
		ok := h.Sketch.Insert(v)
		if ok {
			count++
			utils.ZapLog.Debug("PfAddHandle", zap.String("key", string(k)), zap.String("element", string(v)))
		}
	}
	if count > 0 {
		h.UpdateTime = now
		return redcon.SimpleInt(1)
	}
	return redcon.SimpleInt(0)
}

// PFCOUNT key [key ...]
func (c *Command) PfCountHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(PFCOUNT_COMMAND)
	}
	var count uint64
	for _, v := range args {
		hk := GetHllKey(txn.UserId, txn.DBId, v)
		h := HLogLog.Get(hk)
		if h == nil {
			var err error
			h, err = c.LoadHll(txn, v)
			if err != nil {
				return txn.SetError(err)
			}
			h.Key = hk
			HLogLog.Set(hk, h)
		}
		count += h.Sketch.Estimate()
	}
	return redcon.SimpleInt(count)
}
