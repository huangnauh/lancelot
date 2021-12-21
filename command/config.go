package command

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/json"
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
	if len(args) == 0 {
		return txn.SetWrongArgs(CONFIG_COMMAND)
	}
	subCommand := strings.ToLower(utils.B2S(args[0]))
	switch subCommand {
	case GET_COMMAND:
		return c.ConfigGet(txn, args[1:])
	case GETLOCAL_COMMAND:
		return c.ConfigGetLocal(txn, args[1:])
	case SET_COMMAND:
		return c.ConfigSet(txn, args[1:])
	case DEL_COMMAND:
		return c.ConfigDel(txn, args[1:])
	case RESETSTAT_COMMAND:
		return c.ConfigResetStat(txn, args[1:])
	case REWRITE_COMMAND:
		return c.ScriptRewrite(txn, args[1:])
	default:
		return txn.SetWrongSubArgs(subCommand, ConfigHelpCommand)
	}
}

func printFlat(flatmap map[string]interface{}) []string {
	res := make([]string, 0, len(flatmap))
	for k, v := range flatmap {
		switch v.(type) {
		case string:
			res = append(res, fmt.Sprintf("%s: %s", k, v))
		case int64, float64:
			res = append(res, fmt.Sprintf("%s: %d", k, v))
		case bool:
			res = append(res, fmt.Sprintf("%s: %t", k, v))
		default:
			res = append(res, fmt.Sprintf("%s: %v", k, v))
		}
	}
	return res
}

func (c *Command) ConfigGetLocal(txn *store.Txn, args [][]byte) interface{} {
	if len(args) > 1 {
		return txn.SetWrongSubArgs(GETLOCAL_COMMAND, ConfigHelpCommand)
	}

	ucfg := c.memberlist.GetConfig(txn.UserId)
	if ucfg == nil {
		return nil
	}
	conf := &config.Config{}
	err := json.Unmarshal(ucfg.Value, conf)
	if err != nil {
		utils.ZapLog.Error("get config failed", zap.Uint16("id", txn.UserId),
			zap.ByteString("value", ucfg.Value), zap.Error(err))
		return txn.SetError(err)
	}
	return c.configGet(txn, args, conf)
}

func (c *Command) ConfigGet(txn *store.Txn, args [][]byte) interface{} {
	if len(args) > 1 {
		return txn.SetWrongSubArgs(GET_COMMAND, ConfigHelpCommand)
	}
	return c.configGet(txn, args, txn.Config)
}

func (c *Command) configGet(txn *store.Txn, args [][]byte, config *config.Config) interface{} {
	var str string
	if len(args) > 0 {
		str = strings.ToLower(utils.B2S(args[0]))
	}
	if str == "lua-time-limit" {
		str = "lua.timeout"
	}
	flatmap, err := utils.Flatten(config, str)
	if err != nil {
		return txn.SetError(err)
	}
	utils.ZapLog.Debug("ConfigGet", zap.String("key", str), zap.Any("value", flatmap))
	return printFlat(flatmap)
}

func (c *Command) ConfigDel(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongSubArgs(DEL_COMMAND, ConfigHelpCommand)
	}
	str := strings.ToLower(utils.B2S(args[0]))
	if err := c.SetMemberConfig(txn.UserId, str, nil); err != nil {
		return txn.SetError(err)
	}
	txn.Config = c.GetConfig(txn.UserId)
	return OK
}

func (c *Command) ConfigSet(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 2 {
		return txn.SetWrongSubArgs(SET_COMMAND, ConfigHelpCommand)
	}

	str := strings.ToLower(utils.B2S(args[0]))
	switch str {
	case "lua-time-limit":
		second, err := utils.GetPositiveInt(args[1])
		if err != nil {
			return txn.SetError(xerror.ErrNotInteger)
		}
		sec := time.Duration(second) * time.Second
		if err = c.SetMemberConfig(txn.UserId, "lua.timeout", sec.String()); err != nil {
			return txn.SetError(err)
		}
	default:
		flatmap, err := utils.Flatten(txn.Config, str)
		if len(flatmap) != 1 {
			utils.ZapLog.Warn("ConfigSet", zap.String("key", str), zap.Any("value", flatmap), zap.Error(err))
			return txn.SetWrongSubArgs(SET_COMMAND, ConfigHelpCommand)
		}
		for k, v := range flatmap {
			switch v.(type) {
			case string:
				if err := c.SetMemberConfig(txn.UserId, k, utils.B2S(args[1])); err != nil {
					return txn.SetError(err)
				}
			case bool:
				b, err := strconv.ParseBool(utils.B2S(args[1]))
				if err != nil {
					return txn.SetError(err)
				}
				if err := c.SetMemberConfig(txn.UserId, str, b); err != nil {
					return txn.SetError(err)
				}
			case int, int16, int32, int64, uint, uint16, uint32, uint64:
				num, err := utils.GetPositiveInt(args[1])
				if err != nil {
					return txn.SetError(xerror.ErrNotInteger)
				}
				err = c.SetMemberConfig(txn.UserId, str, num)
				if err != nil {
					return txn.SetError(err)
				}
			default:
				return txn.SetWrongSubArgs(SET_COMMAND, ConfigHelpCommand)

			}
		}

	}
	txn.Config = c.GetConfig(txn.UserId)
	return OK
}

func (c *Command) ConfigResetStat(txn *store.Txn, args [][]byte) interface{} {
	return nil
}

func (c *Command) ScriptRewrite(txn *store.Txn, args [][]byte) interface{} {
	return nil
}

func (c *Command) SetMemberConfig(userID uint16, key string, value interface{}) error {
	if c.memberlist != nil {
		_, err := c.memberlist.SetConfig(userID, key, value)
		if err != nil {
			return err
		}
	}
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
		if rcfg != nil && cfg != nil && rcfg.Version > cfg.Version {
			create = true
		}
		if userID != c.Root.ID {
			ucfg = c.memberlist.GetConfig(userID)
			if ucfg != nil && cfg != nil && ucfg.Version > cfg.Version {
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
				zap.ByteString("value", ucfg.Value), zap.Error(err))
		}
		conf.Version = ucfg.Version
	}
	c.cfgLock.Lock()
	c.cfgs[userID] = &conf
	c.cfgLock.Unlock()
	return &conf
}
