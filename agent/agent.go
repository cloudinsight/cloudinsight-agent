package agent

import (
	"runtime"
	"sync"
	"time"

	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/log"
	"github.com/startover/cloudinsight-agent/common/metric"
	"github.com/startover/cloudinsight-agent/common/plugin"
)

// Agent runs agent and collects data based on the given config
type Agent struct {
	Config    *config.Config
	collector *Collector
}

// NewAgent returns an Agent struct based off the given Config
func NewAgent(conf *config.Config) (*Agent, error) {
	collector := NewCollector(conf)

	a := &Agent{
		Config:    conf,
		collector: collector,
	}

	return a, nil
}

func panicRecover(plugin *plugin.RunningPlugin) {
	if err := recover(); err != nil {
		trace := make([]byte, 2048)
		runtime.Stack(trace, true)
		log.Infof("FATAL: Plugin [%s] panicked: %s, Stack:\n%s\n",
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

	agg := NewAggregator(plugin.Config, metricC)

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

// emit sends the collected metrics to forwarder API
func (a *Agent) emit() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := a.collector.Emit()
		if err != nil {
			log.Infof("Error occured when writing to Forwarder API: %s\n", err.Error())
		}
	}()

	wg.Wait()
}

// emitter monitors the metrics Plugin channel and emits on the minimum interval
func (a *Agent) emitter(shutdown chan struct{}, metricC chan metric.Metric) error {
	// Inelegant, but this sleep is to allow the collect threads to run, so that
	// the emitter will emit after metrics are collected.
	time.Sleep(200 * time.Millisecond)

	ticker := time.NewTicker(30 * time.Second)

	for {
		select {
		case <-shutdown:
			log.Infoln("Hang on, emitting any cached metrics before shutdown")
			a.emit()
			return nil
		case <-ticker.C:
			a.emit()
		case m := <-metricC:
			// log.Infoln(m)
			a.collector.AddMetric(m)
		}
	}
}

// Run runs the agent daemon, collecting every Interval
func (a *Agent) Run(shutdown chan struct{}) error {
	var wg sync.WaitGroup

	// channel shared between all Plugin threads for collecting metrics
	metricC := make(chan metric.Metric, 10000)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.emitter(shutdown, metricC); err != nil {
			log.Infof("emitter routine failed, exiting: %s\n", err.Error())
			close(shutdown)
		}
	}()

	wg.Add(len(a.Config.Plugins))
	for _, p := range a.Config.Plugins {
		go func(rp *plugin.RunningPlugin, interval time.Duration) {
			defer wg.Done()
			if err := a.collect(shutdown, rp, interval, metricC); err != nil {
				log.Info(err.Error())
			}
		}(p, 30*time.Second)
	}

	wg.Wait()
	return nil
}
