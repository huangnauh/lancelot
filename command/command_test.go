package command_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/huangnauh/lancelot/command"
	"github.com/huangnauh/lancelot/config"
	"github.com/huangnauh/lancelot/utils"
)

var cmd *command.Command

func TestMain(m *testing.M) {
	fmt.Println("command test begin")
	config.SetDefaultConfigPermission()
	cfg := config.GetDefaultConfig()
	cmd = command.NewCommand(nil)
	err := cmd.Start()
	if err != nil {
		panic(err)
	}
	utils.SetDevelopmentLog(cfg.LogLevel)
	exitCode := m.Run()
	fmt.Println("command test end")
	os.Exit(exitCode)
}
