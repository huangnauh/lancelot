package command

import (
	"strconv"
	"strings"

	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

const (
	SentinelHelpCommand = "SENTINEL HELP"
)

func (c *Command) SentinelHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetWrongArgs(SENTINEL_COMMAND)
	}

	subCommand := strings.ToLower(utils.B2S(args[0]))
	switch subCommand {
	case MASTER_COMMAND:
		return c.MasterHandle(txn, args[1:])
	case MASTERS_COMMAND:
		return c.MastersHandle(txn, args[1:])
	case MASTERBYNAME_COMMAND:
		return c.MasterByNameHandle(txn, args[1:])
	case SENTINELS_COMMAND:
		return c.SentinelsHandle(txn, args[1:])
	default:
		return txn.SetWrongSubArgs(subCommand, SentinelHelpCommand)
	}
}

func (c *Command) MasterByNameHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(MASTERBYNAME_COMMAND)
	}
	cfg := config.GetDefaultConfig()
	master := strings.ToLower(utils.B2S(args[0]))
	if master != cfg.Sentinel.MasterName {
		return txn.SetError(xerror.ErrNoSuchMaster)
	}
	return []string{cfg.Host, strconv.Itoa(cfg.RedisPort)}
}

func (c *Command) SentinelsHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(SENTINELS_COMMAND)
	}
	master := strings.ToLower(utils.B2S(args[0]))
	cfg := config.GetDefaultConfig()
	if master != cfg.Sentinel.MasterName {
		return txn.SetError(xerror.ErrNoSuchMaster)
	}
	return EmptySlice
}

func (c *Command) GetSentinel(cfg *config.Config) []string {
	slices := make([]string, 28)
	slices[0] = "name"
	slices[1] = cfg.Sentinel.RunID
	slices[2] = "ip"
	slices[3] = cfg.Host
	slices[4] = "port"
	slices[5] = strconv.Itoa(cfg.RedisPort)
	slices[6] = "runid"
	slices[7] = cfg.Sentinel.RunID
	slices[8] = "flags"
	slices[9] = "sentinel"
	slices[10] = "link-pending-commands"
	slices[11] = "0"
	slices[12] = "link-refcount"
	slices[13] = "1"
	slices[14] = "last-ping-sent"
	slices[15] = "0"
	slices[16] = "last-ok-ping-reply"
	slices[17] = "583"
	slices[18] = "last-ping-reply"
	slices[19] = "583"
	slices[20] = "down-after-milliseconds"
	slices[21] = "30000"
	slices[22] = "last-hello-message"
	slices[23] = "1222"
	slices[24] = "voted-leader"
	slices[25] = "?"
	slices[26] = "voted-leader-epoch"
	slices[27] = "0"
	return slices
}

func (c *Command) GetMaster(cfg *config.Config) []string {
	slices := make([]string, 40)
	slices[0] = "name"
	slices[1] = cfg.Sentinel.MasterName
	slices[2] = "ip"
	slices[3] = cfg.Host
	slices[4] = "port"
	slices[5] = strconv.Itoa(cfg.RedisPort)
	slices[6] = "runid"
	slices[7] = cfg.Sentinel.RunID
	slices[8] = "flags"
	slices[9] = "master"
	slices[10] = "link-pending-commands"
	slices[11] = "0"
	slices[12] = "link-refcount"
	slices[13] = "1"
	slices[14] = "last-ping-sent"
	slices[15] = "0"
	slices[16] = "last-ok-ping-reply"
	slices[17] = "0"
	slices[18] = "last-ping-reply"
	slices[19] = "735"
	slices[20] = "down-after-milliseconds"
	slices[21] = "10000"
	slices[22] = "info-refresh"
	slices[23] = "100"
	slices[24] = "role-reported"
	slices[25] = "master"
	slices[26] = "role-reported-time"
	slices[27] = "532439"
	slices[28] = "config-epoch"
	slices[29] = "0"
	slices[30] = "num-slaves"
	slices[31] = "0"
	slices[32] = "num-other-sentinels"
	slices[33] = "0"
	slices[34] = "quorum"
	slices[35] = "0"
	slices[36] = "failover-timeout"
	slices[37] = "60000"
	slices[38] = "parallel-syncs"
	slices[39] = "1"
	return slices
}

func (c *Command) MastersHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 0 {
		return txn.SetWrongArgs(MASTER_COMMAND)
	}
	cfg := config.GetDefaultConfig()
	return [][]string{c.GetMaster(&cfg)}
}

func (c *Command) MasterHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) != 1 {
		return txn.SetWrongArgs(MASTER_COMMAND)
	}
	master := strings.ToLower(utils.B2S(args[0]))
	cfg := config.GetDefaultConfig()
	if master != cfg.Sentinel.MasterName {
		return txn.SetError(xerror.ErrNoSuchMaster)
	}
	return c.GetMaster(&cfg)
}
