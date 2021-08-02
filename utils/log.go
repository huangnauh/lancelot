package utils

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var ZapLog *zap.Logger

func SetProductionLog(level string) {
	l := ConvertToZapLevel(level)
	conf := zap.NewProductionConfig()
	conf.Level.SetLevel(l)
	logger, err := conf.Build()
	if err != nil {
		panic(err)
	}
	ZapLog = logger
}

func SetDevelopmentLog(level string) {
	l := ConvertToZapLevel(level)
	conf := zap.NewDevelopmentConfig()
	conf.Level.SetLevel(l)
	logger, err := conf.Build()
	if err != nil {
		panic(err)
	}
	ZapLog = logger
}

func ConvertToZapLevel(lvl string) zapcore.Level {
	switch lvl {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	case "dpanic":
		return zap.DPanicLevel
	case "panic":
		return zap.PanicLevel
	case "fatal":
		return zap.FatalLevel
	default:
		panic(fmt.Sprintf("unknown level %q", lvl))
	}
}
