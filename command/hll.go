package command

import (
	"encoding/binary"

	"github.com/huangnauh/lancelot/hll"
	"github.com/huangnauh/lancelot/redcon"
	"github.com/huangnauh/lancelot/store"
	"github.com/huangnauh/lancelot/utils"
	"go.uber.org/zap"
)

const DenseSize = 4096

type Dense struct {
	m      uint64
	txn    *store.Txn
	object *Object
	tmp    []uint8
}

func (d *Dense) CheckAndSet(i uint64, rho uint8) (uint8, bool, error) {
	var origin uint8
	var err error
	delta := d.m / DenseSize
	index := i % delta
	bk := make([]byte, 2)
	binary.BigEndian.PutUint16(bk, uint16(i/delta))
	bkey := d.object.GetValueBytes(bk)
	var bvalue []byte
	if d.tmp == nil {
		value, err := d.txn.Get(bkey)
		if err == store.KeyNotFound {
		} else if err != nil {
			return 0, false, err
		} else {
			hvalue := &Value{}
			_ = DecodeValue(value, hvalue)
			bvalue = hvalue.Value
		}
		if len(bvalue) != int(delta) {
			bvalue = make([]uint8, delta)
		}
		origin = bvalue[index]
	} else {
		start := i / delta * delta
		bvalue = d.tmp[start : start+delta]
		origin = bvalue[index]
	}

	set := rho > origin
	if set {
		bvalue[index] = rho
		hvalue := &Value{Value: bvalue, Timestamp: d.txn.Timestamp}
		err = d.txn.Put(bkey, EncodeValue(hvalue))
		if err != nil {
			return 0, false, err
		}
	}
	return origin, set, nil
}

func (d *Dense) Get(i uint64) (uint8, error) {
	bk := make([]byte, 2)
	delta := d.m / DenseSize
	binary.BigEndian.PutUint16(bk, uint16(i/delta))
	bkey := d.object.GetValueBytes(bk)
	var bvalue []byte
	if d.tmp == nil {
		value, err := d.txn.Get(bkey)
		if err == store.KeyNotFound {
			return 0, nil
		} else if err != nil {
			return 0, err
		}
		hvalue := &Value{}
		_ = DecodeValue(value, hvalue)
		bvalue = hvalue.Value
		if len(bvalue) != int(delta) {
			return 0, nil
		}
	} else {
		start := i / delta * delta
		bvalue = d.tmp[start : start+delta]
	}
	index := i % delta
	return bvalue[index], nil
}

func (d *Dense) List() ([]uint8, error) {
	if d.tmp != nil {
		return d.tmp, nil
	}

	start := d.object.GetValueBytes(nil)
	end := utils.PrefixNext(start)
	ret := make([]uint8, d.m)
	delta := int(d.m / DenseSize)
	err := d.txn.List(start, end, d.txn.Config.Redis.ScanMaxCount, func(key, value []byte) bool {
		if len(key) < len(start) {
			return false
		}
		i := int(binary.BigEndian.Uint16(key[len(start):]))
		utils.ZapLog.Debug("list hll", zap.ByteString("key", key), zap.Int("index", i), zap.ByteString("value", value))
		hvalue := &Value{}
		_ = DecodeValue(value, hvalue)
		bvalue := hvalue.Value
		if len(bvalue) != int(delta) {
			return true
		}
		for j := 0; j < len(bvalue); j++ {
			ret[i*delta+j] = bvalue[j]
			// utils.ZapLog.Debug("list hll", zap.Int("index", i*delta+j), zap.Any("value", value[j]))
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	d.tmp = ret
	return ret, nil
}

func getHll(txn *store.Txn, object *Object) (*hll.Plus, error) {
	dense := &Dense{txn: txn, object: object, m: 1 << hll.DefaultPrecision}
	plus, err := hll.NewPlus(hll.DefaultPrecision, dense)
	if err != nil {
		return nil, err
	}
	return plus, nil
}

// PFADD key [element [element ...]]
func (c *Command) PfAddHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(PFADD_COMMAND)
	}
	object, err := c.GetOrCreateUUIDObject(txn, HLLType, args[0])
	if err != nil {
		return txn.SetError(err)
	}
	plus, err := getHll(txn, object)
	if err != nil {
		return txn.SetError(err)
	}
	var ret int
	for i := 1; i < len(args); i++ {
		ok, err := plus.Add(args[i])
		if err != nil {
			return txn.SetError(err)
		}
		if ok {
			ret = 1
		}
	}
	return redcon.SimpleInt(ret)
}

// PFCOUNT key [key ...]
func (c *Command) PfCountHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(PFCOUNT_COMMAND)
	}
	var count uint64
	exists := make(map[string]bool)

	for i := 0; i < len(args); i++ {
		s := utils.B2S(args[i])
		if exists[s] {
			continue
		}
		exists[s] = true
		object := NewObject(txn, HLLType, args[i])
		key := object.GetKeyBytes()
		err := getTxnObject(txn, key, object, true)
		if err == store.KeyNotFound {
			continue
		} else if err != nil {
			return txn.SetError(err)
		}
		plus, err := getHll(txn, object)
		if err != nil {
			return txn.SetError(err)
		}
		l, err := plus.Count()
		if err != nil {
			return txn.SetError(err)
		}
		count += l
	}
	return redcon.SimpleInt(count)
}

// PFMERGE destkey sourcekey [sourcekey ...]
func (c *Command) PfMergeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(PFMERGE_COMMAND)
	}
	destObject, err := c.GetOrCreateUUIDObject(txn, HLLType, args[0])
	if err != nil {
		return txn.SetError(err)
	}
	destPlus, err := getHll(txn, destObject)
	if err != nil {
		return txn.SetError(err)
	}

	exists := make(map[string]bool)
	for _, k := range args[1:] {
		s := utils.B2S(k)
		if exists[s] {
			continue
		}
		exists[s] = true
		object, err := c.GetOrCreateUUIDObject(txn, HLLType, k)
		if err != nil {
			return txn.SetError(err)
		}
		plus, err := getHll(txn, object)
		if err != nil {
			return txn.SetError(err)
		}
		err = destPlus.Merge(plus)
		if err != nil {
			return txn.SetError(err)
		}
	}
	return redcon.SimpleString("OK")
}
