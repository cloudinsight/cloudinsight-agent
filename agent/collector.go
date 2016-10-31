package agent

import (
	"strings"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/api"
	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/emitter"
	"github.com/cloudinsight/cloudinsight-agent/common/gohai"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
)

const metadataUpdateInterval = 4 * time.Hour

// Collector posts metrics to Forwarder API.
type Collector struct {
	*emitter.Emitter

	api   *api.API
	conf  *config.Config
	start time.Time
}

// NewCollector creates a new instance of Collector.
func NewCollector(conf *config.Config) *Collector {
	emitter := emitter.NewEmitter("Collector")
	api := api.NewAPI(conf.GetForwarderAddrWithScheme(), conf.GlobalConfig.LicenseKey, 10*time.Second)

	c := &Collector{
		Emitter: emitter,
		api:     api,
		conf:    conf,
		start:   time.Now(),
	}
	c.Emitter.Parent = c

	return c
}

// Post sends the metrics to Forwarder API.
func (c *Collector) Post(metrics []interface{}) error {
	start := time.Now()
	payload := NewPayload(c.conf)
	payload.Metrics = metrics

	if c.shouldSendMetadata() {
		log.Debug("We should send metadata.")

		payload.Gohai = gohai.GetMetadata()
		if c.conf.GlobalConfig.Tags != "" {
			hostTags := strings.Split(c.conf.GlobalConfig.Tags, ",")
			for i, tag := range hostTags {
				hostTags[i] = strings.TrimSpace(tag)
			}

			if len(hostTags) > 0 {
				payload.HostTags = map[string]interface{}{
					"system": hostTags,
				}
			}
		}
	}

	processes := gohai.GetProcesses()
	if c.IsFirstRun() {
		// When first run, we will retrieve processes to get cpuPercent.
		time.Sleep(1 * time.Second)
		processes = gohai.GetProcesses()
	}

	payload.Processes = map[string]interface{}{
		"processes":  processes,
		"licenseKey": c.conf.GlobalConfig.LicenseKey,
		"host":       c.conf.GetHostname(),
	}

	err := c.api.SubmitMetrics(payload)
	elapsed := time.Since(start)
	if err == nil {
		log.Debugf("Post batch of %d metrics in %s",
			len(metrics), elapsed)
	}
	return err
}

// We send metadata every 4 hours, which contains Gohai, HostTags and so on.
func (c *Collector) shouldSendMetadata() bool {
	if c.IsFirstRun() {
		return true
	}

	if time.Since(c.start) >= metadataUpdateInterval {
		c.start = time.Now()
		return true
	}

	return false
}
