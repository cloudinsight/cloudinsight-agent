package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/cloudinsight/cloudinsight-agent/agent"
	"github.com/cloudinsight/cloudinsight-agent/collector"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins"
	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/forwarder"
	"github.com/cloudinsight/cloudinsight-agent/statsd"
)

func startAgent(shutdown chan struct{}, conf *config.Config) {
	ag := agent.NewAgent(conf)
	err := ag.Run(shutdown)
	if err != nil {
		log.Fatal(err)
	}
}

func startForwarder(shutdown chan struct{}, conf *config.Config) {
	f := forwarder.NewForwarder(conf)
	err := f.Run(shutdown)
	if err != nil {
		log.Fatal(err)
	}
}

func startStatsd(shutdown chan struct{}, conf *config.Config) {
	s := statsd.NewStatsd(conf)
	err := s.Run(shutdown)
	if err != nil {
		log.Fatal(err)
	}
}

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
					log.Infof("Reloading config...")
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

		fmt.Println("Available Plugins:")
		for k := range collector.Plugins {
			fmt.Printf("  %s\n", k)
		}

		log.Infof("Loaded plugins: %s", strings.Join(conf.PluginNames(), " "))

		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()

			startAgent(shutdown, conf)
		}()

		go func() {
			defer wg.Done()

			startForwarder(shutdown, conf)
		}()

		go func() {
			defer wg.Done()

			startStatsd(shutdown, conf)
		}()
		wg.Wait()
	}
}
