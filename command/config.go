package command

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/member"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
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
	if len(args) > 1 {
		return txn.SetWrongSubArgs(GET_COMMAND, ConfigHelpCommand)
	}

	if len(args) == 0 {
		flatmap, err := utils.Flatten(txn.Config)
		if err != nil {
			return txn.SetError(err)
		}
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

	body, err := json.Marshal(txn.Config)
	if err != nil {
		return txn.SetError(err)
	}
	str := strings.ToLower(utils.B2S(args[0]))

	res := gjson.GetBytes(body, str)
	if !res.Exists() {
		return nil
	}
	switch res.Type {
	case gjson.String:
		return redcon.SimpleString(res.String())
	case gjson.Number:
		return redcon.SimpleInt(res.Int())
	default:
		return nil
	}
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
		if err = c.SetMemberConfig(txn.UserId, "lua.timeout", second); err != nil {
			return txn.SetError(err)
		}
	default:
		res := gjson.GetBytes(config.GetDefaultConfigData(), str)
		if !res.Exists() {
			return txn.SetWrongSubArgs(SET_COMMAND, ConfigHelpCommand)
		}
		switch res.Type {
		case gjson.String:
			if err := c.SetMemberConfig(txn.UserId, str, utils.B2S(args[1])); err != nil {
				return txn.SetError(err)
			}
		case gjson.True:
			b, err := strconv.ParseBool(utils.B2S(args[1]))
			if err != nil {
				return txn.SetError(err)
			}
			if err := c.SetMemberConfig(txn.UserId, str, b); err != nil {
				return txn.SetError(err)
			}
		case gjson.Number:
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
				zap.ByteString("value", rcfg.Value), zap.Error(err))
		}
		conf.Version = ucfg.Version
	}
	c.cfgLock.Lock()
	c.cfgs[userID] = &conf
	c.cfgLock.Unlock()
	return &conf
}
