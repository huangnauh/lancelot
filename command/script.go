package command

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	luajson "github.com/layeh/gopher-json"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/redcon"
	lua "github.com/yuin/gopher-lua"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

type LScriptMap struct {
	sync.RWMutex
	scripts map[string]*lua.FunctionProto
}

func (m *LScriptMap) Get(key string) (script *lua.FunctionProto, ok bool) {
	m.RLock()
	script, ok = m.scripts[key]
	m.RUnlock()
	return
}

func (m *LScriptMap) Put(key string, script *lua.FunctionProto) {
	m.Lock()
	m.scripts[key] = script
	m.Unlock()
}

type LStatePool struct {
	sync.Mutex
	saved []*lua.LState
	total int
	init  int
	max   int
}

func NewLStatePool(init, max int) *LStatePool {
	l := &LStatePool{
		saved: make([]*lua.LState, init),
		max:   max,
		init:  init,
		total: init,
	}
	for i := 0; i < init; i++ {
		l.saved[i] = l.New()
	}
	return l
}

func (l *LStatePool) Get() (*lua.LState, error) {
	l.Lock()
	defer l.Unlock()
	n := len(l.saved)
	if n == 0 {
		if l.total >= l.max {
			return nil, xerror.ErrNoLuasAvailable
		}
		l.total++
		return l.New(), nil
	}
	x := l.saved[n-1]
	l.saved = l.saved[0 : n-1]
	return x, nil
}

func (l *LStatePool) Prune() {
	l.Lock()
	defer l.Unlock()
	n := len(l.saved)
	if n > l.init+1 {
		dropNum := (n - l.init) / 2
		newSaved := make([]*lua.LState, n-dropNum)
		copy(newSaved, l.saved[dropNum:])
		l.saved = newSaved
		l.total -= dropNum
	}
}

func getArgs(ls *lua.LState) (cmd string, args [][]byte) {
	cmd = ls.GetGlobal("SCRIPT_CMD").String()
	for i := 1; ; i++ {
		if arg := ls.ToString(i); arg == "" {
			break
		} else {
			args = append(args, utils.S2B(arg))
		}
	}
	return
}

func luaSetRawGlobals(ls *lua.LState, tbl map[string]lua.LValue) {
	gt := ls.Get(lua.GlobalsIndex).(*lua.LTable)
	for key, val := range tbl {
		gt.RawSetString(key, val)
	}
}

func (l *LStatePool) New() *lua.LState {
	L := lua.NewState()
	jsonValue := L.Get(luajson.Loader(L))
	L.SetGlobal("cjson", jsonValue)
	L.SetGlobal("json", jsonValue)
	return L
}

func (l *LStatePool) Put(L *lua.LState) {
	l.Lock()
	l.saved = append(l.saved, L)
	l.Unlock()
}

func (l *LStatePool) Shutdown() {
	l.Lock()
	for _, L := range l.saved {
		L.Close()
	}
	l.Unlock()
}

func errResult(ls *lua.LState, err error, raiseErr bool) int {
	if raiseErr {
		ls.RaiseError(err.Error())
		return 0
	} else {
		ls.Push(covertToLua(ls, err))
		return 1
	}
}

func (c *Command) callTxn(txn *store.Txn, ls *lua.LState, raiseErr bool) int {
	scriptCmd, args := getArgs(ls)
	if len(args) == 0 {
		err := txn.SetError(xerror.WrongArgsError(scriptCmd))
		return errResult(ls, err, raiseErr)
	}

	res, err := c.luaCall(txn, scriptCmd, utils.B2S(args[0]), args[1:])
	if err != nil {
		return errResult(ls, err, raiseErr)
	}
	ls.Push(covertToLua(ls, res))
	return 1
}

func (c *Command) luaCall(txn *store.Txn, scriptCmd, cmd string, args [][]byte) (interface{}, error) {
	cmd = strings.ToLower(cmd)
	switch cmd {
	case "auth", "shutdown", "gc",
		"script load", "script exists", "script flush",
		"eval", "evalsha", "evalro", "evalrosha", "evalna", "evalnasha":
		return nil, xerror.UnsupportCmdFromScript
	}

	txnHandle, ok := c.TxnHandle[cmd]
	if !ok {
		return nil, xerror.UnsupportCmdFromScript
	}
	readonly := strings.HasSuffix(scriptCmd, "ro")
	if readonly && !txnHandle.ReadOnly {
		return nil, xerror.ErrReadOnlyScript
	}
	return txnHandle.Func(txn, args), nil
}

func errorReply(ls *lua.LState) int {
	table := ls.CreateTable(0, 1)
	table.RawSetString("err", lua.LString(ls.ToString(1)))
	ls.Push(table)
	return 1
}

func statusReply(ls *lua.LState) int {
	table := ls.CreateTable(0, 1)
	table.RawSetString("ok", lua.LString(ls.ToString(1)))
	ls.Push(table)
	return 1
}
func sha1hex(ls *lua.LState) int {
	shaSum := utils.Sha1Sum(utils.S2B(ls.ToString(1)))
	ls.Push(lua.LString(shaSum))
	return 1
}

// EVAL script numkeys [key [key ...]] [arg [arg ...]]
// EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
// EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
// EVAL_RO script numkeys key [key ...] arg [arg ...]
func (c *Command) evalHandle(txn *store.Txn, args [][]byte, script_command string) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(EVAL_COMMAND)
	}
	script := args[0]
	numKeysStr := string(args[1])
	numkeysUInt64, err := strconv.ParseUint(numKeysStr, 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}
	numkeys := int(numkeysUInt64)
	if numkeys < 0 {
		return txn.SetError(xerror.ErrNumberNegative)
	}

	if len(args) < numkeys+2 {
		return txn.SetError(xerror.ErrNumberGreater)
	}

	luaState, err := c.luapool.Get()
	if err != nil {
		return txn.SetError(err)
	}
	defer c.luapool.Put(luaState)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	luaState.SetContext(ctx)
	defer luaState.RemoveContext()
	keysTable := luaState.CreateTable(int(numkeys), 0)
	for i := 0; i < numkeys; i++ {
		key := args[2+i]
		keysTable.Append(lua.LString(key))
	}
	luaArgs := args[2+numkeys:]
	argsTable := luaState.CreateTable(len(luaArgs), 0)
	for i := range luaArgs {
		argsTable.Append(lua.LString(luaArgs[i]))
	}

	var exports = map[string]lua.LGFunction{
		"call": func(ls *lua.LState) int {
			return c.callTxn(txn, ls, true)
		},
		"pcall": func(ls *lua.LState) int {
			return c.callTxn(txn, ls, false)
		},
		"error_reply":  errorReply,
		"status_reply": statusReply,
		"sha1hex":      sha1hex,
	}

	luaSetRawGlobals(luaState, map[string]lua.LValue{
		"KEYS":       keysTable,
		"ARGV":       argsTable,
		"SCRIPT_CMD": lua.LString(script_command),
		"redis":      luaState.SetFuncs(luaState.NewTable(), exports),
	})
	defer luaSetRawGlobals(
		luaState, map[string]lua.LValue{
			"KEYS":       lua.LNil,
			"ARGV":       lua.LNil,
			"SCRIPT_CMD": lua.LNil,
			"redis":      lua.LNil,
		})
	var shaSum string

	sha := strings.HasPrefix(script_command, EVALSHA_COMMAND)
	if sha {
		shaSum = utils.B2S(script)
	} else {
		shaSum = utils.Sha1Sum(script)
	}

	proto, ok := c.scriptMap.Get(shaSum)
	var fn *lua.LFunction
	if ok {
		fn = &lua.LFunction{
			IsG:       false,
			Env:       luaState.Env,
			Proto:     proto,
			GFunction: nil,
			Upvalues:  make([]*lua.Upvalue, 0),
		}
	} else if sha {
		return txn.SetError(xerror.ErrNoMatchScript)
	} else {
		fn, err = luaState.Load(bytes.NewReader(script), "s_"+shaSum)
		if err != nil {
			return txn.SetError(xerror.MakeSafeErr(err))
		}
		c.scriptMap.Put(shaSum, fn.Proto)
	}
	luaState.Push(fn)
	if err := luaState.PCall(0, 1, nil); err != nil {
		return txn.SetError(xerror.MakeSafeErr(err))
	}
	ret := luaState.Get(-1)
	luaState.Pop(1)
	return covertLuaValue(ret)
}

func covertToLua(L *lua.LState, val interface{}) lua.LValue {
	logrus.Debugf("covertToLua: %v", val)
	switch val := val.(type) {
	case nil:
		return lua.LNil
	case redcon.SimpleInt:
		return lua.LNumber(val)
	case int:
		return lua.LNumber(val)
	case redcon.SimpleString:
		luaTable := L.CreateTable(0, 1)
		luaTable.RawSetString("ok", lua.LString(val))
		return luaTable
	case string:
		return lua.LString(val)
	case []byte:
		return lua.LString(string(val))
	case error:
		luaTable := L.CreateTable(0, 1)
		luaTable.RawSetString("err", lua.LString(val.Error()))
		return luaTable
	case []interface{}:
		luaTable := L.CreateTable(len(val), 0)
		for _, item := range val {
			luaTable.Append(covertToLua(L, item))
		}
		return luaTable
	default:
		return lua.LNil
	}
}

func covertLuaValue(val lua.LValue) interface{} {
	switch val.Type() {
	case lua.LTNil:
		return nil
	case lua.LTBool:
		if val == lua.LTrue {
			return SimpleInt(1)
		} else {
			return nil
		}
	case lua.LTNumber:
		num := int(val.(lua.LNumber))
		return SimpleInt(num)
	case lua.LTString:
		return val.String()
	case lua.LTTable:
		table := val.(*lua.LTable)
		count := table.Len()
		if count != 0 {
			var values []interface{}
			table.ForEach(func(lk, lv lua.LValue) {
				values = append(values, covertLuaValue(lv))
			})
			return values
		}
		var singleValue interface{}
		table.ForEach(func(lk, lv lua.LValue) {
			if lk.Type() == lua.LTString {
				lks := lk.String()
				switch lks {
				case "ok":
					singleValue = SimpleString(lv.String())
				case "err":
					singleValue = errors.New(lv.String())
				}
			}
		})
		return singleValue
	default:
		return xerror.ErrLuaInvalidType
	}
}
