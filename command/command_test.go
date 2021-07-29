package command_test

import (
	"fmt"
	"testing"

	"gitlab.s.upyun.com/platform/lancelot/command"
	"gitlab.s.upyun.com/platform/lancelot/config"
)

var cmd *command.Command

func TestMain(m *testing.M) {
	fmt.Println("command test begin")
	cfg := config.GetConfig()
	cmd = command.NewCommand(cfg)
	err := cmd.Start()
	if err != nil {
		panic(err)
	}
	fmt.Println("command test end")
}
