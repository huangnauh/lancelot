package command

import (
	"bytes"
	"context"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	lua "github.com/yuin/gopher-lua"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/lua/bit"
	"gitlab.s.upyun.com/platform/lancelot/lua/cjson"
	"gitlab.s.upyun.com/platform/lancelot/lua/cmsgpack"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const (
	ScriptHelpCommand = "SCRIPT HELP"
	StrictScript      = `local dbg=debug
local mt = {}
setmetatable(_G, mt)
mt.__newindex = function (t, n, v)
    if dbg.getinfo(2) then
        local w = dbg.getinfo(2, "S").what
        if w ~= "C" then
            error("Script attempted to create global variable '" .. tostring(n) .. "'", 2)
        end
    end
    rawset(t, n, v)
end
mt.__index = function (t, n)
    if dbg.getinfo(2) then
		local w = dbg.getinfo(2, "S").what
		if w ~= "C" then
        	error("Script attempted to access nonexistent global variable '" ..tostring(n) .. "'", 2);
		end
    end
    return rawget(t, n)
end
debug = nil`
)

type LuaLib struct {
	libName string
	libFunc lua.LGFunction
}

var (
	luaLibs = []LuaLib{
		{lua.LoadLibName, lua.OpenPackage},
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
		// {lua.IoLibName, lua.OpenIo},
		// {lua.OsLibName, lua.OpenOs},
		{lua.StringLibName, lua.OpenString},
		{lua.MathLibName, lua.OpenMath},
		{lua.DebugLibName, lua.OpenDebug},
		{lua.ChannelLibName, lua.OpenChannel},
		{lua.CoroutineLibName, lua.OpenCoroutine},
		{cjson.LibName, cjson.OpenJson},
		{bit.LibName, bit.OpenBit},
		{cmsgpack.LibName, cmsgpack.OpenMsgpack},
	}
)

type LScriptMap struct {
	sync.RWMutex
	scripts map[string]*lua.FunctionProto
}

func (m *LScriptMap) Len() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.scripts)
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
	saved   []*lua.LState
	clock   sync.RWMutex
	cancels map[*lua.LState]context.CancelFunc
	cfg     *config.Lua
	total   int
}

func NewLStatePool(cfg *config.Lua) *LStatePool {
	l := &LStatePool{
		saved:   make([]*lua.LState, cfg.InitPoolSize),
		cancels: make(map[*lua.LState]context.CancelFunc),
		cfg:     cfg,
		total:   cfg.InitPoolSize,
	}
	for i := 0; i < cfg.InitPoolSize; i++ {
		l.saved[i] = l.New()
	}
	return l
}

func (l *LStatePool) Get() (*lua.LState, error) {
	l.Lock()
	defer l.Unlock()
	n := len(l.saved)
	if n == 0 {
		if l.total >= l.cfg.MaxPoolSize {
			return nil, xerror.ErrNoLuasAvailable
		}
		l.total++
		return l.New(), nil
	}
	x := l.saved[n-1]
	l.saved = l.saved[0 : n-1]
	return x, nil
}

func (l *LStatePool) Prune(cfg *config.Lua) {
	l.Lock()
	defer l.Unlock()
	l.cfg = cfg
	n := len(l.saved)
	if n > l.cfg.InitPoolSize+1 {
		dropNum := (n - l.cfg.InitPoolSize) / 2
		newSaved := make([]*lua.LState, n-dropNum)
		copy(newSaved, l.saved[dropNum:])
		l.saved = newSaved
		l.total -= dropNum
	}
}

func getArgs(ls *lua.LState) (cmd string, args [][]byte) {
	cmd = ls.GetGlobal("SCRIPT_CMD").String()
	for i := 1; ; i++ {
		arg := ls.ToString(i)
		if arg == "" {
			break
		}
		args = append(args, utils.S2B(arg))
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
	L := lua.NewState(lua.Options{
		CallStackSize:       lua.CallStackSize,
		RegistrySize:        lua.RegistrySize,
		RegistryMaxSize:     lua.RegistrySize * 2,
		IncludeGoStackTrace: true,
		SkipOpenLibs:        true,
	})
	for _, lib := range luaLibs {
		L.Push(L.NewFunction(lib.libFunc))
		L.Push(lua.LString(lib.libName))
		L.Call(1, 0)
	}
	err := L.DoString(StrictScript)
	if err != nil {
		utils.ZapLog.Error("lua strict script error", zap.Error(err))
	}
	return L
}

func (l *LStatePool) SetCancel(ls *lua.LState, cancel context.CancelFunc) {
	utils.ZapLog.Info("set script cancel")
	l.clock.Lock()
	l.cancels[ls] = cancel
	l.clock.Unlock()
}

func (l *LStatePool) RemoveCancel(ls *lua.LState) {
	utils.ZapLog.Info("remove script cancel")
	l.clock.Lock()
	delete(l.cancels, ls)
	l.clock.Unlock()
}

func (l *LStatePool) GetWorkingScript() int {
	l.clock.RLock()
	num := len(l.cancels)
	l.clock.RUnlock()
	return num
}

func (l *LStatePool) GetCancels() []context.CancelFunc {
	val := make([]context.CancelFunc, 0)
	l.clock.RLock()
	for _, cancel := range l.cancels {
		val = append(val, cancel)
	}
	l.clock.RUnlock()
	return val
}

func (l *LStatePool) Put(L *lua.LState) {
	l.Lock()
	l.saved = append(l.saved, L)
	l.Unlock()
}

func (l *LStatePool) Shutdown() {
	l.clock.Lock()
	for _, cancel := range l.cancels {
		cancel()
	}
	l.clock.Unlock()

	time.Sleep(time.Millisecond * 100)

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
	}
	ls.Push(covertToLua(ls, err))
	return 1
}

func (c *Command) callTxn(txn *store.Txn, ls *lua.LState, raiseErr bool) int {
	scriptCmd, args := getArgs(ls)
	if len(args) == 0 {
		err := txn.SetError(xerror.CallNeedArgsScript)
		return errResult(ls, err, raiseErr)
	}

	res, err := c.luaCall(txn, scriptCmd, utils.B2S(args[0]), args[1:])
	if err != nil {
		return errResult(ls, err, raiseErr)
	}
	val := covertToLua(ls, res)
	utils.ZapLog.Debug("callTxn", zap.Any("res", res), zap.Any("val", val))
	ls.Push(val)
	return 1
}

func (c *Command) luaCall(txn *store.Txn, scriptCmd, cmd string, args [][]byte) (interface{}, error) {
	cmd = strings.ToLower(cmd)
	txnHandle, ok := c.TxnHandle[cmd]
	if !ok {
		return nil, xerror.UnknownCmdFromScript
	}
	if txnHandle.NoSupportScript {
		return nil, xerror.UnsupportCmdFromScript
	}
	readonly := strings.HasSuffix(scriptCmd, "ro")
	if readonly && !txnHandle.ReadOnly {
		return nil, xerror.ErrReadOnlyScript
	}
	resp := txnHandle.Func(txn, args)
	if txn.Err != nil {
		return nil, txn.Err
	}
	return resp, nil
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
	if ls.GetTop() != 1 {
		ls.ArgError(1, "wrong number of arguments")
	}
	shaSum := utils.Sha1Sum(utils.S2B(ls.ToString(1)))
	ls.Push(lua.LString(shaSum))
	return 1
}

// (scripting) SCRIPT LOAD script
func (c *Command) ScriptLoad(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongSubArgs(LOAD_COMMAND, ScriptHelpCommand)
	}
	utils.ZapLog.Info("script load", zap.String("script", utils.B2S(args[0])))
	luapool := c.GetLuaStatePool(txn)
	luaState, err := luapool.Get()
	if err != nil {
		return txn.SetError(err)
	}
	defer luapool.Put(luaState)

	ctx, cancel := context.WithTimeout(context.Background(), txn.Config.Lua.Timeout)
	defer cancel()
	luapool.SetCancel(luaState, cancel)
	defer luapool.RemoveCancel(luaState)
	luaState.SetContext(ctx)
	defer luaState.RemoveContext()
	script := args[0]
	shaSum := utils.Sha1Sum(script)
	_, ok := c.scriptMap.Get(shaSum)
	var fn *lua.LFunction
	if !ok {
		fn, err = luaState.Load(bytes.NewReader(script), "s_"+shaSum)
		if err != nil {
			return txn.SetError(xerror.MakeSafeErr(err))
		}
		c.scriptMap.Put(shaSum, fn.Proto)
	}
	return shaSum
}

// (scripting) SCRIPT EXISTS sha1 [sha1 ...]
func (c *Command) ScriptExists(txn *store.Txn, args [][]byte) interface{} {
	results := make([]int, len(args))
	for i := range args {
		_, ok := c.scriptMap.Get(strings.ToLower(utils.B2S(args[i])))
		if ok {
			results[i] = 1
		} else {
			results[i] = 0
		}
	}
	return results
}

// (scripting) SCRIPT FLUSH [ASYNC|SYNC]
func (c *Command) ScriptFlush(txn *store.Txn, args [][]byte) interface{} {
	if len(args) > 1 {
		return txn.SetError(xerror.UnsupportFlushOption)
	}

	if len(args) == 1 {
		opt := strings.ToLower(utils.B2S(args[0]))
		if opt != ASYNC_OPTION && opt != SYNC_OPTION {
			return txn.SetError(xerror.UnsupportFlushOption)
		}
	}
	c.scriptMap.Lock()
	c.scriptMap.scripts = make(map[string]*lua.FunctionProto)
	c.scriptMap.Unlock()
	return OK
}

// SCRIPT KILL
func (c *Command) ScriptKill(txn *store.Txn, args [][]byte) interface{} {
	utils.ZapLog.Info("Script Kill")
	if len(args) != 0 {
		return txn.SetWrongSubArgs(KILL_COMMAND, ScriptHelpCommand)
	}
	luapool := c.GetLuaStatePool(txn)
	cancels := luapool.GetCancels()
	for _, cancel := range cancels {
		cancel()
	}
	return OK
}

func (c *Command) ScriptHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(SCRIPT_COMMAND)
	}

	subCommand := strings.ToLower(utils.B2S(args[0]))
	switch subCommand {
	case LOAD_COMMAND:
		return c.ScriptLoad(txn, args[1:])
	case EXISTS_COMMAND:
		return c.ScriptExists(txn, args[1:])
	case KILL_COMMAND:
		return c.ScriptKill(txn, args[1:])
	case FLUSH_COMMAND:
		return c.ScriptFlush(txn, args[1:])
	default:
		return txn.SetWrongSubArgs(subCommand, ScriptHelpCommand)
	}
}

// EVAL script numkeys [key [key ...]] [arg [arg ...]]
// EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]
// EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
// EVAL_RO script numkeys key [key ...] arg [arg ...]
func (c *Command) evalHandle(txn *store.Txn, args [][]byte, script_command string) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(script_command)
	}
	utils.ZapLog.Info("evalHandle", zap.String("script", utils.B2S(args[0])))
	script := args[0]
	numKeysStr := string(args[1])
	numkeysInt64, err := strconv.ParseInt(numKeysStr, 10, 64)
	if err != nil {
		return txn.SetError(xerror.ErrNotInteger)
	}

	numkeys := int(numkeysInt64)
	if numkeys < 0 {
		return txn.SetError(xerror.ErrNumberNegative)
	}

	if len(args) < numkeys+2 {
		return txn.SetError(xerror.ErrNumberGreater)
	}

	luapool := c.GetLuaStatePool(txn)
	luaState, err := luapool.Get()
	if err != nil {
		return txn.SetError(err)
	}
	defer luapool.Put(luaState)
	ctx, cancel := context.WithTimeout(context.Background(), txn.Config.Lua.Timeout)
	defer cancel()
	luapool.SetCancel(luaState, cancel)
	defer luapool.RemoveCancel(luaState)
	luaState.SetContext(ctx)
	defer luaState.RemoveContext()
	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				err = utils.ConnCheck(txn.NetConn())
				if err != nil {
					utils.ZapLog.Warn("conn check error", zap.Error(err),
						zap.String("remote", txn.RemoteAddr()))
					cancel()
				}
				return
			case <-ctx.Done():
				return
			}
		}
	}()
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

	// FIXME:
	originDB := txn.DBId
	defer func() {
		txn.DBId = originDB
	}()

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
		shaSum = strings.ToLower(utils.B2S(script))
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
		emsg := err.Error()
		if strings.Contains(emsg, "context canceled") {
			return txn.SetError(xerror.ErrScriptKilled)
		} else if strings.Contains(emsg, "context deadline exceeded") {
			return txn.SetError(xerror.ErrScriptTimeout)
		}
		return txn.SetError(xerror.MakeSafeErr(err))
	}
	ret := luaState.Get(-1)
	luaState.Pop(1)
	val := covertLuaValue(ret)
	utils.ZapLog.Debug("covert to go", zap.Any("ret", ret), zap.Any("val", val))
	return val
}

func covertToLua(L *lua.LState, val interface{}) lua.LValue {
	utils.ZapLog.Debug("covert to lua", zap.Any("val", val))
	switch val := val.(type) {
	case nil:
		return lua.LBool(false)
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
	case *xerror.RedisError:
		luaTable := L.CreateTable(0, 1)
		luaTable.RawSetString("err", lua.LString(val.StructError()))
		return luaTable
	case error:
		luaTable := L.CreateTable(0, 1)
		luaTable.RawSetString("err", lua.LString(val.Error()))
		return luaTable
	default:
		vv := reflect.ValueOf(val)
		if vv.Kind() == reflect.Slice {
			luaTable := L.CreateTable(vv.Len(), 0)
			for i := 0; i < vv.Len(); i++ {
				vvv := covertToLua(L, vv.Index(i).Interface())
				utils.ZapLog.Debug("covert to lua", zap.Any("value", vvv), zap.Any("index", vv.Index(i).Interface()))
				luaTable.Append(vvv)
			}
			utils.ZapLog.Debug("covert to lua", zap.Int("value", vv.Len()), zap.Int("table", luaTable.Len()))
			return luaTable
		}
		return lua.LBool(false)
	}
}

func covertLuaValue(val lua.LValue) interface{} {
	utils.ZapLog.Debug("covert to go", zap.Any("val", val))
	switch val.Type() {
	case lua.LTNil:
		return nil
	case lua.LTBool:
		if val == lua.LTrue {
			return SimpleInt(1)
		}
		return nil
	case lua.LTNumber:
		num := int64(val.(lua.LNumber))
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
					singleValue = xerror.MakeSafe(lv.String())
				}
			}
		})
		return singleValue
	default:
		return xerror.ErrLuaInvalidType
	}
}
