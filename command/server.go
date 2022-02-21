package command

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/version"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

func (c *Command) getFlushUserId(txn *store.Txn, args [][]byte) (uint16, error) {
	if txn.UserId != c.Root.ID && !txn.Config.FlushPermission {
		return 0, xerror.WrongPermissionError(FLUSH_COMMAND)
	}

	deleteId := txn.UserId
	if len(args) == 1 {
		str := strings.ToLower(string(args[0]))
		switch str {
		case "async", "sync":
		default:
			if txn.UserId == c.Root.ID {
				userId, err := strconv.Atoi(str)
				if err == nil {
					deleteId = uint16(userId)
				} else {
					user, ok := c.GetLocalUser(str)
					if !ok {
						return 0, xerror.WrongUsernameError(str)
					}
					deleteId = user.ID
				}
			}
		}
	}
	return deleteId, nil
}

// FLUSHALL [ASYNC|SYNC]
func (c *Command) FlushAllHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) > 1 {
		return txn.SetWrongArgs(FLUSHALL_COMMAND)
	}

	deleteId, err := c.getFlushUserId(txn, args)
	if err != nil {
		return txn.SetError(err)
	}

	start := GetUserPrefix(DataPrefix, deleteId)
	end := utils.PrefixNext(start)
	ctx := context.Background()
	err = c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}

	start = GetUserPrefix(CountPrefix, deleteId)
	end = utils.PrefixNext(start)
	ctx = context.Background()
	err = c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}

	start = GetUserPrefix(TTLPrefix, deleteId)
	end = utils.PrefixNext(start)
	err = c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}
	return OK
}

// FLUSHDB [ASYNC|SYNC]
func (c *Command) FlushDBHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) > 1 {
		return txn.SetWrongArgs(FLUSHDB_COMMAND)
	}

	deleteId, err := c.getFlushUserId(txn, args)
	if err != nil {
		return txn.SetError(err)
	}

	start := GetUserDBPrefix(DataPrefix, deleteId, txn.DBId)
	end := utils.PrefixNext(start)
	ctx := context.Background()
	err = c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}

	start = GetUserDBPrefix(CountPrefix, deleteId, txn.DBId)
	end = utils.PrefixNext(start)
	ctx = context.Background()
	err = c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}

	start = GetUserDBPrefix(TTLPrefix, deleteId, txn.DBId)
	end = utils.PrefixNext(start)
	err = c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}
	return OK
}

// TIME
func (c *Command) TimeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) > 0 {
		return txn.SetWrongArgs(TIME_COMMAND)
	}
	now := txn.Now
	return []string{strconv.Itoa(int(now / 1000)), strconv.Itoa(int(now % 1000))}
}

// DBSIZE
func (c *Command) DBSizeHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) > 0 {
		return txn.SetWrongArgs(DBSIZE_COMMAND)
	}
	counts, err := ListCount(txn, txn.UserId, txn.DBId, CountGeneral, KEYSIZE)
	if err != nil {
		return txn.SetError(err)
	}
	count := int64(0)
	for _, v := range counts {
		count += v.Value
	}
	return redcon.SimpleInt(count)
}

// func (c *Command) ComamndHandle(txn *store.Txn, args [][]byte) interface{} {
// 	if len(args) == 0 {

// 	}
// }

// func (c *Command) commandHandle(txn *store.Txn, args [][]byte) interface{} {
// 	for com := range c.TxnHandle {

// 	}
// }

// DEBUG OBJECT key

func (c *Command) RoleHandle(txn *store.Txn, args [][]byte) interface{} {
	return []interface{}{"master", "21625300", []string{}}
}

func (c *Command) InfoHandle(txn *store.Txn, args [][]byte) interface{} {
	exe, err := os.Executable()
	if err != nil {
		return txn.SetError(err)
	}

	subcommand := ""
	if len(args) >= 1 {
		subcommand = strings.ToLower(string(args[0]))
	}

	var b strings.Builder
	if subcommand == "" || subcommand == "server" {
		b.WriteString("# Server\r\n")
		b.WriteString("redis_version:6.2.4\r\n")

		b.WriteString("redis_git_sha1:")
		b.WriteString(version.GitCommit)
		b.WriteString("\r\n")
		b.WriteString("redis_build_id:462e443fe1573a8b\r\n")
		b.WriteString("redis_mode:standalone\r\n")

		b.WriteString("os:")
		b.WriteString(runtime.GOOS)
		b.WriteString("\r\n")

		b.WriteString("arch_bits:")
		b.WriteString(runtime.GOARCH)
		b.WriteString("\r\n")

		b.WriteString("process_id:")
		b.WriteString(strconv.Itoa(os.Getpid()))
		b.WriteString("\r\n")

		b.WriteString("tcp_port:")
		b.WriteString(strconv.Itoa(txn.Config.RedisPort))
		b.WriteString("\r\n")

		since := time.Since(txn.Config.StartAt)
		b.WriteString("uptime_in_seconds:")
		b.WriteString(strconv.Itoa(int(since / time.Second)))
		b.WriteString("\r\n")
		b.WriteString("uptime_in_days:")
		b.WriteString(strconv.Itoa(int(since / time.Hour / 24)))
		b.WriteString("\r\n")
		b.WriteString("executable:")
		b.WriteString(exe)
		b.WriteString("\r\n")
	}

	if subcommand == "" || subcommand == "clients" {
		b.WriteString("# Clients\r\n")
		b.WriteString("connected_clients:")
		b.WriteString(strconv.FormatInt(atomic.LoadInt64(&c.Info.ConnectedClients), 10))
		b.WriteString("\r\n")
		b.WriteString("client_longest_output_list:0\r\n")
		b.WriteString("client_biggest_input_buf:0\r\n")
		b.WriteString("blocked_clients:")
		b.WriteString(strconv.FormatInt(atomic.LoadInt64(&c.Info.BlockClients), 10))
		b.WriteString("\r\n")
	}

	if subcommand == "" || subcommand == "memory" {
		num := c.GetCachedScript()
		b.WriteString("# Memory\r\n")
		b.WriteString("number_of_cached_scripts:")
		b.WriteString(strconv.Itoa(num))
		b.WriteString("\r\n")
	}

	if subcommand == "" || subcommand == "gc" {
		b.WriteString("# GC\r\n")
		info, err := c.GetGCInfo()
		if err != nil {
			b.WriteString(xerror.MakeSafeString(err.Error()))
		} else {
			b.WriteString("gc_last_time:")
			b.WriteString(info.LastTime.String())
			b.WriteString("\r\n")
		}

		b.WriteString("\r\n")
	}

	if subcommand == "" || subcommand == "health" {
		b.WriteString("# Health\r\n")
		b.WriteString("health:")
		b.WriteString(strconv.FormatBool(c.Info.Health))
		b.WriteString("\r\n")
	}
	if subcommand == "" || subcommand == "lancelot" {
		b.WriteString("# Lancelot\r\n")
		b.WriteString("lancelot_version:")
		b.WriteString(version.RedisVersion())
		b.WriteString("\r\n")

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		leader := c.client.GetLeader(ctx)
		b.WriteString("leader:")
		b.WriteString(leader)
		b.WriteString("\r\n")
		serverId := c.client.ID()
		b.WriteString("id:")
		b.WriteString(serverId)
		b.WriteString("\r\n")
	}
	return b.String()
}
