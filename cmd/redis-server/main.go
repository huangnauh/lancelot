package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudflare/tableflip"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/server"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
)

var (
	showVersion bool
	configFile  string
	dev         bool
)

func main() {
	flag.BoolVar(&showVersion, "version", false, "print version string and quit")
	flag.StringVar(&configFile, "config", "./conf.yaml", "configuration filename")
	flag.BoolVar(&dev, "dev", false, "development")
	flag.Parse()

	err := config.LoadYAMLConfig(configFile)
	if err != nil {
		panic(err)
	}

	cfg := config.GetConfig()
	if !dev {
		utils.SetProductionLog(cfg.LogLevel)
	} else {
		utils.SetDevelopmentLog(cfg.LogLevel)
	}

	upg, err := tableflip.New(tableflip.Options{
		UpgradeTimeout: time.Minute,
		PIDFile:        cfg.PIDFile,
	})
	if err != nil {
		utils.ZapLog.Fatal("Server init Upgrade failed", zap.Error(err))
	}
	defer upg.Stop()
	// Do an upgrade on SIGHUP
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGHUP, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
		for s := range sig {
			if s == syscall.SIGHUP {
				err := upg.Upgrade()
				if err != nil {
					utils.ZapLog.Error("Server Upgrade failed", zap.Error(err))
				}
			} else {
				upg.Stop()
			}
		}
	}()

	// Listen must be called before Ready
	redln, err := upg.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort))
	if err != nil {
		utils.ZapLog.Fatal("Redis Server listen failed", zap.Error(err))
	}
	httpln, err := upg.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.HttpPort))
	if err != nil {
		utils.ZapLog.Fatal("Http Server listen failed", zap.Error(err))
	}
	rpcln, err := upg.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.RpcPort))
	if err != nil {
		utils.ZapLog.Fatal("Rpc Server listen failed", zap.Error(err))
	}
	serv := server.NewServer(cfg)
	serv.Start(httpln, redln, rpcln)

	if err := upg.Ready(); err != nil {
		panic(err)
	}
	<-upg.Exit()

	utils.ZapLog.Info("Shutdown...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	serv.Shutdown(ctx)
	_ = utils.ZapLog.Sync()
}
