package log

import (
	"log"

	"gitlab.s.upyun.com/platform/lancelot/config"
	"gopkg.in/natefinch/lumberjack.v2"
)

func InitAccessLog(cfg *config.Log) {
	if cfg.Filename == "" {
		return
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(&lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxNum,
		MaxAge:     cfg.MaxAge,
		LocalTime:  true,
	})
}
