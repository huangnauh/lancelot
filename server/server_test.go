package server_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/huangnauh/lancelot/config"
	"github.com/huangnauh/lancelot/server"
	"github.com/huangnauh/lancelot/utils"
	"go.uber.org/goleak"
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
	config.SetDefaultConfigPermission()
	conf := config.GetDefaultConfig()
	cfg = &conf
	utils.SetDevelopmentLog(cfg.LogLevel)
	var err error
	ser, err = server.NewServer(cfg)
	if err != nil {
		panic(err)
	}
	redln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort))
	if err != nil {
		panic(err)
	}

	err = ser.Command.Start()
	if err != nil {
		panic(err)
	}
	go ser.RedisServe(redln)

	exitCode := m.Run()
	ser.Shutdown(context.Background())
	fmt.Println("server test end")
	if exitCode == 0 {
		if err := goleak.Find(
			goleak.IgnoreTopFunction("sync.runtime_notifyListWait"),
			goleak.IgnoreTopFunction("github.com/klauspost/compress/zstd.(*blockDec).startDecoder"),
			goleak.IgnoreTopFunction("github.com/onsi/ginkgo/internal/specrunner.(*SpecRunner).registerForInterrupts"),
			goleak.IgnoreTopFunction("github.com/pingcap/goleveldb/leveldb.(*DB).mpoolDrain"),
			goleak.IgnoreTopFunction("github.com/pingcap/goleveldb/leveldb.(*DB).tCompaction"),
			goleak.IgnoreTopFunction("github.com/pingcap/goleveldb/leveldb/util.(*BufferPool).drain"),
			goleak.IgnoreTopFunction("github.com/pingcap/goleveldb/leveldb.(*DB).mCompaction"),
			goleak.IgnoreTopFunction("github.com/pingcap/goleveldb/leveldb.(*DB).compactionError"),
			goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
			goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccResolverWrapper).watcher"),
			goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).createTransport"),
			goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
			goleak.IgnoreTopFunction("google.golang.org/grpc.(*Server).handleRawConn"),
			goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
			goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)
			exitCode = 1
		}
	}
	os.Exit(exitCode)
}
