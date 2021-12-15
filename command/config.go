package command

import (
	"encoding/json"
	"strings"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/member"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

const (
	ConfigHelpCommand = "CONFIG HELP"
)

// CONFIG SET parameter value
func (c *Command) ConfigHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(CONFIG_COMMAND)
	}
	subCommand := strings.ToLower(utils.B2S(args[0]))
	switch subCommand {
	case GET_COMMAND:
		return c.ConfigGet(txn, args[1:])
	case SET_COMMAND:
		return c.ConfigSet(txn, args[1:])
	case RESETSTAT_COMMAND:
		return c.ConfigResetStat(txn, args[1:])
	case REWRITE_COMMAND:
		return c.ScriptRewrite(txn, args[1:])
	default:
		return txn.SetWrongSubArgs(subCommand, ConfigHelpCommand)
	}
}

func (c *Command) ConfigGet(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongSubArgs(GET_COMMAND, ConfigHelpCommand)
	}
	return nil
}

func (c *Command) ConfigSet(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongSubArgs(SET_COMMAND, ConfigHelpCommand)
	}
	switch strings.ToLower(utils.B2S(args[0])) {
	case "lua-time-limit":
		second, err := utils.GetPositiveInt(args[1])
		if err != nil {
			return txn.SetError(xerror.ErrNotInteger)
		}
		txn.Config.Lua.Timeout = time.Second * time.Duration(second)
		return OK
	default:
		return txn.SetWrongSubArgs(SET_COMMAND, ConfigHelpCommand)
	}
}

func (c *Command) ConfigResetStat(txn *store.Txn, args [][]byte) interface{} {
	return nil
}

func (c *Command) ScriptRewrite(txn *store.Txn, args [][]byte) interface{} {
	return nil
}

func (c *Command) GetConfig(userID uint16) *config.Config {
	c.cfgLock.RLock()
	cfg, ok := c.cfgs[userID]
	c.cfgLock.RUnlock()
	create := !ok

	var rcfg, ucfg *member.Config
	if c.memberlist != nil {
		rcfg = c.memberlist.GetConfig(c.Root.ID)
		if rcfg != nil && rcfg.Version > cfg.Version {
			create = true
		}
		if userID != c.Root.ID {
			ucfg = c.memberlist.GetConfig(userID)
			if ucfg != nil && ucfg.Version > cfg.Version {
				create = true
			}
		}
	}

	if !create {
		return cfg
	}

	conf := config.GetDefaultConfig()
	if rcfg != nil {
		err := json.Unmarshal(rcfg.Value, &conf)
		if err != nil {
			utils.ZapLog.Error("get config failed", zap.Uint16("id", c.Root.ID),
				zap.ByteString("value", rcfg.Value), zap.Error(err))
		}
		conf.Version = rcfg.Version
	}
	if ucfg != nil {
		err := json.Unmarshal(ucfg.Value, &conf)
		if err != nil {
			utils.ZapLog.Error("get config failed", zap.Uint16("id", userID),
				zap.ByteString("value", rcfg.Value), zap.Error(err))
		}
	}
	c.cfgLock.Lock()
	c.cfgs[userID] = &conf
	c.cfgLock.Unlock()
	return &conf
}
