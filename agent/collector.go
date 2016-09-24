package agent

import (
	"strings"
	"time"

	"github.com/startover/cloudinsight-agent/common/api"
	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/gohai"
	"github.com/startover/cloudinsight-agent/common/log"
	"github.com/startover/cloudinsight-agent/common/metric"
)

const (
	// DefaultMetricBatchSize is default size of metrics batch size.
	DefaultMetricBatchSize = 2000

	// DefaultMetricBufferLimit is default number of metrics kept. It should be a multiple of batch size.
	DefaultMetricBufferLimit = 10000

	metadataUpdateInterval = 4 * time.Hour
)

// Collector contains the output configuration
type Collector struct {
	api               *api.API
	config            *config.Config
	MetricBufferLimit int
	MetricBatchSize   int

	metrics     *Buffer
	failMetrics *Buffer
	start       time.Time
	emitCount   int
}

// NewCollector XXX
func NewCollector(conf *config.Config) *Collector {
	api := api.NewAPI(conf.GetForwarderAddrWithScheme(), conf.GlobalConfig.LicenseKey, 5*time.Second)
	bufferLimit := DefaultMetricBufferLimit
	batchSize := DefaultMetricBatchSize

	c := &Collector{
		api:               api,
		config:            conf,
		metrics:           NewBuffer(batchSize),
		failMetrics:       NewBuffer(bufferLimit),
		MetricBufferLimit: bufferLimit,
		MetricBatchSize:   batchSize,
		start:             time.Now(),
	}
	return c
}

// AddMetric adds a metric to the Collector. It will post metrics to Forwarder
// when the metrics size has reached the MetricBatchSize.
func (c *Collector) AddMetric(metric metric.Metric) {
	c.metrics.Add(metric)
	if c.metrics.Len() == c.MetricBatchSize {
		batch := c.metrics.Batch(c.MetricBatchSize)
		err := c.post(batch)
		if err != nil {
			c.failMetrics.Add(batch...)
		}
	}
}

// Emit writes all cached metrics to Forwarder.
func (c *Collector) Emit() error {
	log.Infof("Buffer fullness: %d / %d metrics. "+
		"Total gathered metrics: %d. Total dropped metrics: %d.",
		c.failMetrics.Len()+c.metrics.Len(),
		c.MetricBufferLimit,
		c.metrics.Total(),
		c.metrics.Drops()+c.failMetrics.Drops())

	c.emitCount++
	var err error
	if !c.failMetrics.IsEmpty() {
		bufLen := c.failMetrics.Len()
		// how many batches of failed metrics we need to post.
		nBatches := bufLen/c.MetricBatchSize + 1
		batchSize := c.MetricBatchSize

		for i := 0; i < nBatches; i++ {
			// If it's the last batch, only grab the metrics that have not had
			// a post attempt already (this is primarily to preserve order).
			if i == nBatches-1 {
				batchSize = bufLen % c.MetricBatchSize
			}
			batch := c.failMetrics.Batch(batchSize)
			// If we've already failed previous Emit, don't bother trying to
			// post to Forwarder again. We are not exiting the loop just so
			// that we can rotate the metrics to preserve order.
			if err == nil {
				err = c.post(batch)
			}
			if err != nil {
				c.failMetrics.Add(batch...)
			}
		}
	}

	batch := c.metrics.Batch(c.MetricBatchSize)
	// if c.failMetrics is empty then err will always be nil at this point.
	if err == nil {
		err = c.post(batch)
	}
	if err != nil {
		c.failMetrics.Add(batch...)
		return err
	}
	return nil
}

func (c *Collector) post(metrics []metric.Metric) error {
	if metrics == nil || len(metrics) == 0 {
		return nil
	}

	formattedMetrics := c.format(metrics)
	if len(formattedMetrics) == 0 {
		return nil
	}

	start := time.Now()
	payload := api.NewPayload(c.config)
	payload.Metrics = formattedMetrics

	if c.shouldSendMetadata() {
		log.Info("We should send metadata.")
		payload.Gohai = gohai.GetMetadata()

		if c.config.GlobalConfig.Tags != "" {
			hostTags := strings.Split(c.config.GlobalConfig.Tags, ",")
			for i, tag := range hostTags {
				hostTags[i] = strings.TrimSpace(tag)
			}

			payload.HostTags = map[string]interface{}{
				"system": hostTags,
			}
		}
	}

	processes := gohai.GetProcesses()
	if c.isFirstRun() {
		// When first run, we will retrieve processes to get cpuPercent.
		time.Sleep(1 * time.Second)
		processes = gohai.GetProcesses()
	}

	payload.Processes = map[string]interface{}{
		"processes":  processes,
		"licenseKey": c.config.GlobalConfig.LicenseKey,
		"host":       c.config.GetHostname(),
	}

	err := c.api.Post(payload)
	elapsed := time.Since(start)
	if err == nil {
		log.Infof("Write batch of %d metrics in %s\n",
			len(metrics), elapsed)
	}
	return err
}

func (c *Collector) format(metrics []metric.Metric) []interface{} {
	m := make([]interface{}, len(metrics))
	for i, metric := range metrics {
		m[i] = metric.Format()
	}
	return m
}

func (c *Collector) isFirstRun() bool {
	return c.emitCount <= 1
}

func (c *Collector) shouldSendMetadata() bool {
	if c.isFirstRun() {
		return true
	}

	if time.Since(c.start) >= metadataUpdateInterval {
		c.start = time.Now()
		return true
	}

	return false
}
