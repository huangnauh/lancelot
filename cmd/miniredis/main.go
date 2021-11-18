package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/alicebob/miniredis/v2"
)

func main() {
	_, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	for _ = range sig {
		return
	}
}
