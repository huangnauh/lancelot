package command

import (
	"math"
	"strconv"
	"strings"

	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

func (c *Command) getStartEnd(txn *store.Txn, args [][]byte) (*lOpt, error) {
	opt := &lOpt{count: 0, max: txn.Config.Redis.ScanMaxCount}
	start, err := strconv.ParseInt(utils.B2S(args[0]), 10, 64)
	if err != nil {
		return opt, xerror.ErrNotInteger
	}
	end, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return opt, xerror.ErrNotInteger
	}
	opt.index = []int64{start, end}
	opt.max, err = c.checkMaxLen(txn, args[2:])
	if err != nil {
		return opt, err
	}
	return opt, nil
}

// (list) LMOVE source destination LEFT|RIGHT LEFT|RIGHT
func (c *Command) LMoveHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 4 {
		return txn.SetWrongArgs(LMOVE_COMMAND)
	}
	sstr := strings.ToLower(utils.B2S(args[2]))
	if sstr != "left" && sstr != "right" {
		return txn.SetError(xerror.ErrSyntax)
	}
	dstr := strings.ToLower(utils.B2S(args[3]))
	if dstr != "left" && dstr != "right" {
		return txn.SetError(xerror.ErrSyntax)
	}
	var value interface{}
	var err error
	if sstr == "left" {
		value, err = c.ListHandle(txn, [][]byte{args[0]}, LPOP_COMMAND, &lOpt{exist: true})
	} else {
		value, err = c.ListHandle(txn, [][]byte{args[0]}, RPOP_COMMAND, &lOpt{exist: true})
	}
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	msg, ok := value.([]byte)
	if !ok {
		return nil
	}
	if dstr == "left" {
		_, err = c.ListHandle(txn, [][]byte{args[1], msg}, LPUSH_COMMAND, &lOpt{})
	} else {
		_, err = c.ListHandle(txn, [][]byte{args[1], msg}, RPUSH_COMMAND, &lOpt{})
	}
	if err != nil {
		return txn.SetError(err)
	}
	return value
}

//(list) RPOPLPUSH source destination
func (c *Command) RPopLPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongArgs(RPOPLPUSH_COMMAND)
	}
	value, err := c.ListHandle(txn, [][]byte{args[0]}, RPOP_COMMAND, &lOpt{exist: true})
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	msg, ok := value.([]byte)
	if !ok {
		return nil
	}
	_, err = c.ListHandle(txn, [][]byte{args[1], msg}, LPUSH_COMMAND, &lOpt{})
	if err != nil {
		return txn.SetError(err)
	}
	return value
}

// (list) LRANGE key start stop [MAXLEN len]
func (c *Command) LRangeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(LRANGE_COMMAND)
	}
	opt, err := c.getStartEnd(txn, args[1:])
	if err != nil {
		return txn.SetError(err)
	}
	opt.exist = true
	opt.readonly = true
	ret, err := c.ListHandle(txn, args, LRANGE_COMMAND, opt)
	if err == store.KeyNotFound {
		return []redcon.SimpleInt{}
	}
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LLEN key
func (c *Command) LLenHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(LLEN_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, LLEN_COMMAND, &lOpt{exist: true, readonly: true})
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	}
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// (list) LTRIM key start stop [MAXLEN len]
func (c *Command) LTrimHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(LTRIM_COMMAND)
	}
	opt, err := c.getStartEnd(txn, args[1:])
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.ListHandle(txn, args, LTRIM_COMMAND, opt)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// (list) LINFO key [start end]
func (c *Command) LInfoHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 3 {
		return txn.SetWrongArgs(LINFO_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, LINFO_COMMAND, &lOpt{exist: true, max: txn.Config.Redis.ScanMaxCount})
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

// (list) LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
func (c *Command) LPosHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 || len(args)%2 != 0 {
		return txn.SetWrongArgs(LPOS_COMMAND)
	}
	opt := &lOpt{count: 0, max: txn.Config.Redis.ScanMaxCount, index: []int64{0}, exist: true, readonly: true}
	var err error
	for i := 2; i < len(args); i += 2 {
		switch strings.ToLower(utils.B2S(args[i])) {
		case "rank":
			index, err := strconv.ParseInt(utils.B2S(args[i+1]), 10, 64)
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
			if index == 0 {
				return txn.SetError(xerror.ErrRankZero)
			}
			opt.index = []int64{index}
		case "count":
			opt.count, err = strconv.Atoi(utils.B2S(args[i+1]))
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
			if opt.count < 0 {
				return txn.SetError(xerror.ErrCountNegative)
			}
			if opt.count == 0 {
				opt.count = math.MaxInt64
			}
		case "maxlen":
			opt.max, err = utils.GetPositiveInt(args[i+1])
			if err != nil {
				return txn.SetError(xerror.ErrNotInteger)
			}
		default:
			return txn.SetWrongArgs(LPOS_COMMAND)
		}
	}

	ret, err := c.ListHandle(txn, args, LPOS_COMMAND, opt)
	if err == store.KeyNotFound || err == store.ReachLimit {
		if opt.count > 0 {
			return []redcon.SimpleInt{}
		}
		return nil
	}
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func (c *Command) checkMaxLen(txn *store.Txn, args [][]byte) (int, error) {
	if len(args) > 0 {
		str := strings.ToLower(utils.B2S(args[4]))
		if str != "maxlen" || len(args) != 2 {
			return 0, xerror.ErrSyntax
		}
		max, err := utils.GetPositiveInt(args[5])
		if err != nil {
			return 0, err
		}
		return max, nil
	}
	return txn.Config.Redis.ScanMaxCount, nil
}

//(list) LREM key count element [MAXLEN len]
func (c *Command) LRemHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(LREM_COMMAND)
	}
	opt := &lOpt{}
	var err error
	opt.count, err = strconv.Atoi(utils.B2S(args[1]))
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	opt.max, err = c.checkMaxLen(txn, args[3:])
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.ListHandle(txn, args, LREM_COMMAND, opt)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LINSERT key BEFORE|AFTER pivot element [MAXLEN len]
func (c *Command) LInsertHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 4 {
		return txn.SetWrongArgs(LINSERT_COMMAND)
	}
	str := strings.ToLower(utils.B2S(args[1]))
	opt := &lOpt{exist: true}
	if str == "before" {
		opt.before = true
	} else if str == "after" {
		opt.after = true
	} else {
		return txn.SetError(xerror.ErrSyntax)
	}

	var err error
	opt.max, err = c.checkMaxLen(txn, args[4:])
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.ListHandle(txn, args, LINSERT_COMMAND, opt)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LINDEX key index [MAXLEN len]
func (c *Command) LIndexHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(LINDEX_COMMAND)
	}
	index, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	opt := &lOpt{index: []int64{index}, readonly: true, exist: true}
	opt.max, err = c.checkMaxLen(txn, args[2:])
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.ListHandle(txn, args, LINDEX_COMMAND, opt)
	if err == store.KeyNotFound {
		return nil
	}
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LSET key index element [MAXLEN len]
func (c *Command) LSetHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetWrongArgs(LSET_COMMAND)
	}
	index, err := strconv.ParseInt(utils.B2S(args[1]), 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	opt := &lOpt{index: []int64{index}, exist: true}
	opt.max, err = c.checkMaxLen(txn, args[3:])
	if err != nil {
		return txn.SetError(err)
	}
	ret, err := c.ListHandle(txn, args, LSET_COMMAND, opt)
	if err == store.KeyNotFound {
		return txn.SetError(xerror.ErrNoSuchKey)
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) RPUSHX key element [element ...]
func (c *Command) RPushXHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(RPUSHX_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, RPUSH_COMMAND, &lOpt{exist: true})
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) RPUSH key element [element ...]
func (c *Command) RPushHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(RPUSH_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, RPUSH_COMMAND, &lOpt{})
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LPUSHX key element [element ...]
func (c *Command) LPushXHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(LPUSHX_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, LPUSH_COMMAND, &lOpt{exist: true})
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) LPUSH key element [element ...]
func (c *Command) LPushHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(LPUSH_COMMAND)
	}
	ret, err := c.ListHandle(txn, args, LPUSH_COMMAND, &lOpt{})
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func (c *Command) listMany(txn *store.Txn, args [][]byte, cmd string) (interface{}, error) {
	opt := &lOpt{exist: true}
	utils.ZapLog.Debug("block list", zap.ByteStrings("args", args))
	for i := 0; i < len(args); i++ {
		ret, err := c.ListHandle(txn, [][]byte{args[i]}, cmd, opt)
		if err == store.KeyNotFound {
			continue
		} else if err != nil {
			return nil, err
		}
		msg, ok := ret.([]byte)
		if !ok {
			continue
		}
		return [][]byte{args[i], msg}, nil
	}
	return nil, nil
}

//(list) BRPOP key [key ...] timeout
func (c *Command) BrPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(BRPOP_COMMAND)
	}
	bfunc := func(txn *store.Txn, args [][]byte) (interface{}, error) {
		return c.listMany(txn, args, RPOP_COMMAND)
	}
	ret, err := c.BlockHandle(txn, args, bfunc)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
func (c *Command) BlMoveHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 5 {
		return txn.SetWrongArgs(BLMOVE_COMMAND)
	}
	sstr := strings.ToLower(utils.B2S(args[2]))
	if sstr != "left" && sstr != "right" {
		return txn.SetError(xerror.ErrSyntax)
	}
	dstr := strings.ToLower(utils.B2S(args[3]))
	if dstr != "left" && dstr != "right" {
		return txn.SetError(xerror.ErrSyntax)
	}
	var value interface{}
	var err error
	sargs := [][]byte{args[0], args[4]}
	if sstr == "left" {
		value, err = c.BlockHandle(txn, sargs, func(txn *store.Txn, args [][]byte) (interface{}, error) {
			return c.listMany(txn, sargs, LPOP_COMMAND)
		})
	} else {
		value, err = c.BlockHandle(txn, sargs, func(txn *store.Txn, args [][]byte) (interface{}, error) {
			return c.listMany(txn, sargs, RPOP_COMMAND)
		})
	}
	if err != nil {
		return txn.SetError(err)
	}
	msg, ok := value.([][]byte)
	if !ok {
		return nil
	}
	if len(msg) != 2 {
		return nil
	}
	if dstr == "left" {
		_, err = c.ListHandle(txn, [][]byte{args[1], msg[1]}, LPUSH_COMMAND, &lOpt{})
	} else {
		_, err = c.ListHandle(txn, [][]byte{args[1], msg[1]}, RPUSH_COMMAND, &lOpt{})
	}
	if err != nil {
		return txn.SetError(err)
	}
	return msg[1]
}

//(list) BRPOPLPUSH source destination timeout
func (c *Command) BRPopLPushHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 3 {
		return txn.SetWrongArgs(BRPOPLPUSH_COMMAND)
	}
	sargs := [][]byte{args[0], args[2]}
	bfunc := func(txn *store.Txn, args [][]byte) (interface{}, error) {
		return c.listMany(txn, args, RPOP_COMMAND)
	}
	value, err := c.BlockHandle(txn, sargs, bfunc)
	if err != nil {
		return txn.SetError(err)
	}
	msg, ok := value.([][]byte)
	if !ok {
		return nil
	}
	if len(msg) != 2 {
		return nil
	}
	_, err = c.ListHandle(txn, [][]byte{args[1], msg[1]}, LPUSH_COMMAND, &lOpt{})
	if err != nil {
		return txn.SetError(err)
	}
	return msg[1]
}

//(list) BLPOP key [key ...] timeout
func (c *Command) BlPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(BLPOP_COMMAND)
	}
	bfunc := func(txn *store.Txn, args [][]byte) (interface{}, error) {
		return c.listMany(txn, args, LPOP_COMMAND)
	}
	ret, err := c.BlockHandle(txn, args, bfunc)
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

//(list) RPOP key [count]
func (c *Command) RPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(RPOP_COMMAND)
	}
	return c.pophandle(txn, args, RPOP_COMMAND)
}

//(list) LPOP key [count]
func (c *Command) LPopHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 && len(args) != 2 {
		return txn.SetWrongArgs(LPOP_COMMAND)
	}
	return c.pophandle(txn, args, LPOP_COMMAND)
}

func (c *Command) pophandle(txn *store.Txn, args [][]byte, cmd string) interface{} {
	opt := &lOpt{exist: true}
	if len(args) == 2 {
		count, err := utils.GetPositiveInt(args[1])
		if err == utils.ErrInvalidInt && count == 0 {
			return nil
		}
		if err != nil {
			return txn.SetError(xerror.ErrNotPositiveInteger)
		}
		opt.count = count
	}
	ret, err := c.ListHandle(txn, args, cmd, opt)
	if err == store.KeyNotFound {
		return nil
	}
	if err != nil {
		return txn.SetError(err)
	}
	return ret
}

func (c *Command) ListHandle(txn *store.Txn, args [][]byte, cmd string, opt *lOpt) (interface{}, error) {
	if txn.Config.Redis.ListType == config.BLIST {
		lFunc, ok := BListFuncs[cmd]
		if !ok {
			return nil, xerror.UnknownCommandError(cmd)
		}
		return c.BListHandle(txn, args, lFunc, opt)
	} else {
		lFunc, ok := AListFuncs[cmd]
		if !ok {
			return nil, xerror.UnknownCommandError(cmd)
		}
		return c.AListHandle(txn, args, lFunc, opt)
	}
}
