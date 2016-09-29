package agent

import (
	"runtime"
	"sync"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
)

// Agent runs agent and collects data based on the given config
type Agent struct {
	conf      *config.Config
	collector *Collector
}

// NewAgent returns an Agent struct based off the given Config
func NewAgent(conf *config.Config) *Agent {
	collector := NewCollector(conf)

	a := &Agent{
		conf:      conf,
		collector: collector,
	}

	return a
}

func panicRecover(plugin *plugin.RunningPlugin) {
	if err := recover(); err != nil {
		trace := make([]byte, 2048)
		runtime.Stack(trace, true)
		log.Infof("FATAL: Plugin [%s] panicked: %s, Stack:\n%s",
			plugin.Name, err, trace)
	}
}

// collect runs the Plugins that have been configured with their own
// reporting interval.
func (a *Agent) collect(
	shutdown chan struct{},
	plugin *plugin.RunningPlugin,
	interval time.Duration,
	metricC chan metric.Metric,
) error {
	defer panicRecover(plugin)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	agg := NewAggregator(metricC, a.conf)

	for {
		collectWithTimeout(shutdown, plugin, agg, interval)

		select {
		case <-shutdown:
			return nil
		case <-ticker.C:
			continue
		}
	}
}

// collectWithTimeout collects from the given Plugin, with the given timeout.
//   when the given timeout is reached, and logs an error message
//   but continues waiting for it to return. This is to avoid leaving behind
//   hung processes, and to prevent re-calling the same hung process over and
//   over.
func collectWithTimeout(
	shutdown chan struct{},
	plugin *plugin.RunningPlugin,
	agg metric.Aggregator,
	timeout time.Duration,
) {
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()
	done := make(chan error)
	go func() {
		for _, instance := range plugin.Config.Instances {
			done <- plugin.Plugin.Check(agg, instance)
			agg.Flush()
		}
	}()

	for {
		select {
		case err := <-done:
			if err != nil {
				log.Infof("ERROR in plugin [%s]: %s", plugin.Name, err)
			}
			return
		case <-ticker.C:
			log.Infof("ERROR: plugin [%s] took longer to collect than "+
				"collection interval (%s)",
				plugin.Name, timeout)
			continue
		case <-shutdown:
			return
		}
	}
}

// Test verifies that we can 'collect' from all Plugins with their configured
// Config struct
func (a *Agent) Test() error {
	return nil
}

// Run runs the agent daemon, collecting every Interval
func (a *Agent) Run(shutdown chan struct{}) error {
	var wg sync.WaitGroup
	interval := 30 * time.Second

	// channel shared between all Plugin threads for collecting metrics
	metricC := make(chan metric.Metric, 10000)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.collector.Run(shutdown, metricC, interval); err != nil {
			log.Infof("Collector routine failed, exiting: %s", err.Error())
			close(shutdown)
		}
	}()

	wg.Add(len(a.conf.Plugins))
	for _, p := range a.conf.Plugins {
		go func(rp *plugin.RunningPlugin, interval time.Duration) {
			defer wg.Done()
			if err := a.collect(shutdown, rp, interval, metricC); err != nil {
				log.Info(err.Error())
			}
		}(p, interval)
	}

	wg.Wait()
	return nil
}
