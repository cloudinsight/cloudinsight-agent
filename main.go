package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/startover/cloudinsight-agent/agent"
	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/log"
	"github.com/startover/cloudinsight-agent/forwarder"
)

func main() {
	reload := make(chan bool, 1)
	reload <- true
	for <-reload {
		reload <- false

		shutdown := make(chan struct{})
		signals := make(chan os.Signal)
		signal.Notify(signals, os.Interrupt, syscall.SIGHUP)
		go func() {
			select {
			case sig := <-signals:
				if sig == os.Interrupt {
					close(shutdown)
				}
				if sig == syscall.SIGHUP {
					log.Infof("Reloading config...\n")
					<-reload
					reload <- true
					close(shutdown)
				}
			}
		}()

		conf, err := config.NewConfig()
		if err != nil {
			log.Fatalf("failed to load config: %s", err)
		}

		err = conf.InitializeLogging()
		if err != nil {
			log.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			agent.Start(shutdown, conf)
		}()

		go func() {
			defer wg.Done()
			forwarder.Start(shutdown, conf)
		}()
		wg.Wait()
	}
}
