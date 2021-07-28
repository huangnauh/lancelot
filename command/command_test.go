package command

import (
	"fmt"
	"testing"

	"gitlab.s.upyun.com/platform/lancelot/config"
)

var cmd *Command

func TestMain(m *testing.M) {
	fmt.Println("command test begin")
	cfg := config.GetConfig()
	cmd = NewCommand(cfg)
	err := cmd.Start()
	if err != nil {
		panic(err)
	}
	fmt.Println("command test end")
}
