package server_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/server"
	"gitlab.s.upyun.com/platform/lancelot/utils"
)

var ser *server.Server
var cfg *config.Config

type testString struct {
	Key     string
	Value   string
	Expire  time.Duration
	KeepTTL bool
}

var testRedisString = []testString{
	{"string", "bar", 0, false},
	{"stringkeep", "bar", 0, true},
	{"string1s", "bar", time.Second, false},
	{"string2s", "bar", 2 * time.Second, false},
	{"string1skeep", "bar", time.Second, true},
	{"string2skeep", "bar", 2 * time.Second, true},
	{"string100ms", "bar", 100 * time.Millisecond, false},
	{"string200ms", "bar", 200 * time.Millisecond, false},
	{"string100mskeep", "bar", 100 * time.Millisecond, true},
	{"string200mskeep", "bar", 200 * time.Millisecond, true},
}

type testHash struct {
	Key     string
	Field   string
	Value   string
	Expire  time.Duration
	KeepTTL bool
}

func TestMain(m *testing.M) {
	fmt.Println("server test begin")
	cfg = config.GetConfig()
	utils.SetDevelopmentLog(cfg.LogLevel)
	ser = server.NewServer(cfg)
	redln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort))
	if err != nil {
		panic(err)
	}

	err = ser.Command.Start()
	if err != nil {
		panic(err)
	}
	go ser.RedisServe(redln)

	exitVal := m.Run()
	ser.Shutdown(context.Background())
	fmt.Println("server test end")
	os.Exit(exitVal)
}
