package rft

import (
	"io"
	"os"
	"os/signal"
	"syscall"
)

func HandleSignals(funcs ...io.Closer) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigs
		for _, f := range funcs {
			f.Close()
		}
		os.Exit(1)
	}()

}
