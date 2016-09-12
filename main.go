package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/startover/cloudinsight-agent/agent"
	"github.com/startover/cloudinsight-agent/collector"
	_ "github.com/startover/cloudinsight-agent/collector/plugins"
	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/log"
)

func start(conf *config.Config) {
	fmt.Println(conf.GlobalConfig.CiURL)
	fmt.Println(conf.GlobalConfig.LicenseKey)
	fmt.Println(conf.GlobalConfig)
	fmt.Println(conf.LoggingConfig)

	// Test log level setting
	log.Debug("Debug")
	log.Info("Info")
	log.Warn("Warn")
	log.Error("Error")
	// log.Fatal("Fatal")

	reload := make(chan bool, 1)
	reload <- true
	for <-reload {
		reload <- false

		ag, err := agent.NewAgent(conf)
		if err != nil {
			log.Fatal(err)
		}

		err = ag.Connect()
		if err != nil {
			log.Fatal(err)
		}

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

		log.Infoln("Available Plugins:")
		for k := range collector.Plugins {
			fmt.Printf("  %s\n", k)
		}
		log.Infof("Loaded plugins: %s", strings.Join(conf.PluginNames(), " "))

		err = ag.Run(shutdown)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func getConfigPath() string {
	return fmt.Sprint("cloudinsight-agent.conf")
}

// resolveConfig loads config file to return config.Config information.
func resolveConfig() (*config.Config, error) {
	conf := config.NewConfig()

	confPath := getConfigPath()
	err := conf.LoadConfig(confPath)
	if err != nil {
		return nil, fmt.Errorf("Failed to load the config file: %s", err)
	}

	if conf.GlobalConfig.LicenseKey == "" {
		// configLogger.Errorf("LicenseKey is empty.")
		log.Error("LicenseKey is empty.")
		return nil, fmt.Errorf("LicenseKey must be specified in the config file.")
	}
	return conf, nil
}

func main() {
	conf, err := resolveConfig()
	if err != nil {
		log.Fatalf("failed to load config: %s", err)
	}

	err = conf.InitializeLogging()
	if err != nil {
		log.Fatal(err)
	}

	start(conf)
}
