package agent

import (
	"fmt"
	"strings"

	"github.com/startover/cloudinsight-agent/collector"
	_ "github.com/startover/cloudinsight-agent/collector/plugins"
	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/log"
)

func Start(shutdown chan struct{}, conf *config.Config) {
	ag, err := NewAgent(conf)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Available Plugins:")
	for k := range collector.Plugins {
		fmt.Printf("  %s\n", k)
	}

	log.Infof("Loaded plugins: %s", strings.Join(conf.PluginNames(), " "))

	err = ag.Run(shutdown)
	if err != nil {
		log.Fatal(err)
	}
}
