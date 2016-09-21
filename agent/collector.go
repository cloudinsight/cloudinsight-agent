package agent

import (
	"time"

	"github.com/startover/cloudinsight-agent/common/api"
	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/log"
	"github.com/startover/cloudinsight-agent/common/metric"
)

const (
	// DefaultMetricBatchSize is default size of metrics batch size.
	DefaultMetricBatchSize = 1000

	// DefaultMetricBufferLimit is default number of metrics kept. It should be a multiple of batch size.
	DefaultMetricBufferLimit = 10000
)

// Collector contains the output configuration
type Collector struct {
	api               *api.API
	config            *config.Config
	MetricBufferLimit int
	MetricBatchSize   int

	metrics     *Buffer
	failMetrics *Buffer
}

// NewCollector XXX
func NewCollector(config *config.Config) *Collector {
	api := api.NewAPI("http://127.0.0.1:9999", config.GlobalConfig.LicenseKey, time.Second*5)
	bufferLimit := DefaultMetricBufferLimit
	batchSize := DefaultMetricBatchSize

	c := &Collector{
		api:               api,
		config:            config,
		metrics:           NewBuffer(batchSize),
		failMetrics:       NewBuffer(bufferLimit),
		MetricBufferLimit: bufferLimit,
		MetricBatchSize:   batchSize,
	}
	return c
}

// AddMetric adds a metric to the output. This function can also write cached
// points if FlushBufferWhenFull is true.
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

// Emit writes all cached points to this output.
func (c *Collector) Emit() error {
	log.Infof("Buffer fullness: %d / %d metrics. "+
		"Total gathered metrics: %d. Total dropped metrics: %d.",
		c.failMetrics.Len()+c.metrics.Len(),
		c.MetricBufferLimit,
		c.metrics.Total(),
		c.metrics.Drops()+c.failMetrics.Drops())

	var err error
	if !c.failMetrics.IsEmpty() {
		bufLen := c.failMetrics.Len()
		// how many batches of failed writes we need to write.
		nBatches := bufLen/c.MetricBatchSize + 1
		batchSize := c.MetricBatchSize

		for i := 0; i < nBatches; i++ {
			// If it's the last batch, only grab the metrics that have not had
			// a write attempt already (this is primarily to preserve order).
			if i == nBatches-1 {
				batchSize = bufLen % c.MetricBatchSize
			}
			batch := c.failMetrics.Batch(batchSize)
			// If we've already failed previous writes, don't bother trying to
			// write to this output again. We are not exiting the loop just so
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
	// see comment above about not trying to write to an already failed output.
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
	payload := api.NewPayload(c.config, formattedMetrics)
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
