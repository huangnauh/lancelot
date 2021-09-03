package command

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/version"
)

func (c *Command) FlushAllHandle(txn *store.Txn, args [][]byte) interface{} {
	start := GetDataUserPrefix(txn.UserId)
	end := utils.PrefixNext(start)
	ctx := context.Background()
	err := c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}

	start = GetCountUserPrefix(txn.UserId)
	end = utils.PrefixNext(start)
	err = c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}
	return OK
}

func (c *Command) FlushDBHandle(txn *store.Txn, args [][]byte) interface{} {
	start := GetUserDBPrefix(txn.UserId, txn.DBId)
	end := utils.PrefixNext(start)
	ctx := context.Background()
	err := c.client.UnsafeDeleteRange(ctx, start, end, 2)
	if err != nil {
		return txn.SetError(err)
	}
	return OK
}

func (c *Command) InfoHandle(txn *store.Txn, args [][]byte) interface{} {
	exe, err := os.Executable()
	if err != nil {
		return txn.SetError(err)
	}

	var b strings.Builder
	b.WriteString("# Server\n")
	b.WriteString("redis_version:6.2.4\n")

	b.WriteString("redis_git_sha1:")
	b.WriteString(version.GitCommit)
	b.WriteString("\n")
	b.WriteString("redis_build_id:462e443fe1573a8b\n")
	b.WriteString("redis_mode:standalone\n")

	b.WriteString("os:")
	b.WriteString(runtime.GOOS)
	b.WriteString("\n")

	b.WriteString("arch_bits:")
	b.WriteString(runtime.GOARCH)
	b.WriteString("\n")

	b.WriteString("process_id:")
	b.WriteString(strconv.Itoa(os.Getpid()))
	b.WriteString("\n")

	b.WriteString("tcp_port:")
	b.WriteString(strconv.Itoa(c.cfg.RedisPort))
	b.WriteString("\n")

	since := time.Since(c.cfg.StartAt)
	b.WriteString("uptime_in_seconds:")
	b.WriteString(strconv.Itoa(int(since / time.Second)))
	b.WriteString("\n")
	b.WriteString("uptime_in_days:")
	b.WriteString(strconv.Itoa(int(since / time.Hour / 24)))
	b.WriteString("\n")
	b.WriteString("executable:")
	b.WriteString(exe)
	b.WriteString("\n")

	b.WriteString("# Clients\n")
	b.WriteString("connected_clients:0\n")
	b.WriteString("client_longest_output_list:0\n")
	b.WriteString("client_biggest_input_buf:0\n")
	b.WriteString("blocked_clients:0\n")
	return b.String()
}
