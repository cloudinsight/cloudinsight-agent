package agent

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"time"

	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/log"
	"github.com/startover/cloudinsight-agent/common/metric"
	"github.com/startover/cloudinsight-agent/common/plugin"
	"github.com/startover/cloudinsight-agent/forwarder"
)

// Agent runs agent and collects data based on the given config
type Agent struct {
	Config  *config.Config
	handler *forwarder.Handler
}

// NewAgent returns an Agent struct based off the given Config
func NewAgent(config *config.Config) (*Agent, error) {
	handler := forwarder.NewHandler(config)

	a := &Agent{
		Config:  config,
		handler: handler,
	}

	return a, nil
}

// Connect connects to Forwarder API
func (a *Agent) Connect() error {
	err := a.handler.API.Connect()
	if err != nil {
		log.Infof("Failed to connect to Forwarder API, retrying in 15s, "+
			"error was '%s' \n", err)
		time.Sleep(15 * time.Second)
		err = a.handler.API.Connect()
		if err != nil {
			return err
		}
	}

	return nil
}

func panicRecover(plugin *plugin.RunningPlugin) {
	if err := recover(); err != nil {
		trace := make([]byte, 2048)
		runtime.Stack(trace, true)
		log.Infof("FATAL: Plugin [%s] panicked: %s, Stack:\n%s\n",
			plugin.Name, err, trace)
		log.Infoln("PLEASE REPORT THIS PANIC ON GITHUB with " +
			"stack trace, configuration, and OS information: " +
			"https://github.com/startover/cloudinsight-agent/issues/new")
	}
}

// gatherer runs the Plugins that have been configured with their own
// reporting interval.
func (a *Agent) gatherer(
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
		var outerr error

		gatherWithTimeout(shutdown, plugin, agg, interval)

		if outerr != nil {
			return outerr
		}

		select {
		case <-shutdown:
			return nil
		case <-ticker.C:
			continue
		}
	}
}

// gatherWithTimeout gathers from the given Plugin, with the given timeout.
//   when the given timeout is reached, gatherWithTimeout logs an error message
//   but continues waiting for it to return. This is to avoid leaving behind
//   hung processes, and to prevent re-calling the same hung process over and
//   over.
func gatherWithTimeout(
	shutdown chan struct{},
	plugin *plugin.RunningPlugin,
	agg metric.Aggregator,
	timeout time.Duration,
) {
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()
	done := make(chan error)
	go func() {
		fmt.Println("instances:", len(plugin.Config.Instances))
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

// Test verifies that we can 'Gather' from all Plugins with their configured
// Config struct
func (a *Agent) Test() error {
	return nil
}

// flush writes a list of metrics to all configured outputs
func (a *Agent) flush() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := a.handler.Write()
		if err != nil {
			log.Infof("Error occured when writing to Forwarder API: %s\n", err.Error())
		}
	}()

	wg.Wait()
}

// flusher monitors the metrics Plugin channel and flushes on the minimum interval
func (a *Agent) flusher(shutdown chan struct{}, metricC chan metric.Metric) error {
	// Inelegant, but this sleep is to allow the Gather threads to run, so that
	// the flusher will flush after metrics are collected.
	time.Sleep(time.Millisecond * 200)

	ticker := time.NewTicker(30 * time.Second)

	for {
		select {
		case <-shutdown:
			log.Infoln("Hang on, flushing any cached metrics before shutdown")
			a.flush()
			return nil
		case <-ticker.C:
			randomSleep(5, shutdown)
			a.flush()
		case m := <-metricC:
			// log.Infoln(m)
			a.handler.AddMetric(m)
		}
	}
}

// RandomSleep will sleep for a random amount of time up to max.
// If the shutdown channel is closed, it will return before it has finished
// sleeping.
func randomSleep(max time.Duration, shutdown chan struct{}) {
	if max == 0 {
		return
	}
	maxSleep := big.NewInt(max.Nanoseconds())

	var sleepns int64
	if j, err := rand.Int(rand.Reader, maxSleep); err == nil {
		sleepns = j.Int64()
	}

	t := time.NewTimer(time.Nanosecond * time.Duration(sleepns))
	select {
	case <-t.C:
		return
	case <-shutdown:
		t.Stop()
		return
	}
}

// Run runs the agent daemon, gathering every Interval
func (a *Agent) Run(shutdown chan struct{}) error {
	var wg sync.WaitGroup

	// channel shared between all Plugin threads for accumulating metrics
	metricC := make(chan metric.Metric, 10000)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.flusher(shutdown, metricC); err != nil {
			log.Infof("Flusher routine failed, exiting: %s\n", err.Error())
			close(shutdown)
		}
	}()

	wg.Add(len(a.Config.Plugins))
	for _, p := range a.Config.Plugins {
		go func(in *plugin.RunningPlugin, interv time.Duration) {
			defer wg.Done()
			if err := a.gatherer(shutdown, in, interv, metricC); err != nil {
				log.Info(err.Error())
			}
		}(p, 30*time.Second)
	}

	wg.Wait()
	return nil
}
