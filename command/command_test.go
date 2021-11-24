package command_test

import (
	"fmt"
	"os"
	"testing"

	"gitlab.s.upyun.com/platform/lancelot/command"
	"gitlab.s.upyun.com/platform/lancelot/config"
	"gitlab.s.upyun.com/platform/lancelot/utils"
)

var cmd *command.Command

func TestMain(m *testing.M) {
	fmt.Println("command test begin")
	cfg := config.GetConfig()
	cmd = command.NewCommand(cfg, nil)
	err := cmd.Start()
	if err != nil {
		panic(err)
	}
	utils.SetDevelopmentLog(cfg.LogLevel)
	exitCode := m.Run()
	fmt.Println("command test end")
	os.Exit(exitCode)
}
