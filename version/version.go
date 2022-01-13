package version

import (
	"fmt"
	"runtime"
)

const (
	APP = "lancelot"
)

var (
	GitCommit   = "UNKNOWN"
	GitDescribe = "UNKNOWN"
)

func RedisVersion() string {
	return fmt.Sprintf("%s (%s)", GitDescribe, GitCommit)
}

func Version() string {
	return fmt.Sprintf("%s version %s (%s), runtime:%s/%s %s",
		APP, GitDescribe, GitCommit, runtime.GOOS, runtime.GOARCH, runtime.Version())
}
