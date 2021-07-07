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
	"github.com/sirupsen/logrus"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/server"
)

var (
	showVersion bool
	configFile  string
)

func main() {
	flag.BoolVar(&showVersion, "version", false, "print version string and quit")
	flag.StringVar(&configFile, "config", "./conf.yaml", "configuration filename")
	flag.Parse()

	err := config.LoadYAMLConfig(configFile)
	if err != nil {
		logrus.Fatalf("load config: %v", err)
	}

	cfg := config.GetConfig()
	level, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = logrus.ErrorLevel
	}
	logrus.SetLevel(level)

	upg, err := tableflip.New(tableflip.Options{
		UpgradeTimeout: time.Minute,
		PIDFile:        cfg.PIDFile,
	})
	if err != nil {
		logrus.Fatalf("Server init Upgrade failed: %v", err)
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
					logrus.Errorf("Server Upgrade failed: %v", err)
				}
			} else {
				upg.Stop()
			}
		}
	}()

	// Listen must be called before Ready
	redln, err := upg.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.RedisPort))
	if err != nil {
		logrus.Fatalln("Redis Server Can't listen :", err)
	}
	httpln, err := upg.Listen("tcp", fmt.Sprintf("%s:%d", cfg.Host, cfg.HttpPort))
	if err != nil {
		logrus.Fatalln("Http Server Can't listen :", err)
	}
	serv := server.NewServer(cfg)

	err = serv.OpenStore()
	if err != nil {
		logrus.Fatalln("OpenStore:", err)
	}

	go func() {
		serv.HttpServe(httpln)
	}()

	go func() {
		serv.RedisServe(redln)
	}()

	if err := upg.Ready(); err != nil {
		panic(err)
	}
	<-upg.Exit()

	logrus.Debugf("Shutdown...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	serv.Shutdown(ctx)
}
