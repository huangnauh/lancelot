package log

import (
	"errors"
	"log"
	"os"

	"gitlab.s.upyun.com/platform/lancelot/config"
	"gopkg.in/natefinch/lumberjack.v2"
)

func InitAccessLog(cfg *config.Log) error {
	if cfg.Filename == "" {
		return nil
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	info, err := os.Stat(cfg.Filename)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return errors.New("log file is a directory")
	}

	log.SetOutput(&lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxNum,
		MaxAge:     cfg.MaxAge,
		LocalTime:  true,
	})
	return nil
}
