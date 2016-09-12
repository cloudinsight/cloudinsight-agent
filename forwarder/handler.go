package forwarder

import (
	"time"

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

// Handler contains the output configuration
type Handler struct {
	API               *API
	MetricBufferLimit int
	MetricBatchSize   int

	metrics     *Buffer
	failMetrics *Buffer
}

// NewHandler XXX
func NewHandler(config *config.Config) *Handler {
	api := NewAPI(config.GlobalConfig.CiURL, config.GlobalConfig.LicenseKey, time.Second*5)
	bufferLimit := DefaultMetricBufferLimit
	batchSize := DefaultMetricBatchSize

	h := &Handler{
		API:               api,
		metrics:           NewBuffer(batchSize),
		failMetrics:       NewBuffer(bufferLimit),
		MetricBufferLimit: bufferLimit,
		MetricBatchSize:   batchSize,
	}
	return h
}

// AddMetric adds a metric to the output. This function can also write cached
// points if FlushBufferWhenFull is true.
func (h *Handler) AddMetric(metric metric.Metric) {
	h.metrics.Add(metric)
	if h.metrics.Len() == h.MetricBatchSize {
		batch := h.metrics.Batch(h.MetricBatchSize)
		err := h.write(batch)
		if err != nil {
			h.failMetrics.Add(batch...)
		}
	}
}

// Write writes all cached points to this output.
func (h *Handler) Write() error {
	log.Infof("Buffer fullness: %d / %d metrics. "+
		"Total gathered metrics: %d. Total dropped metrics: %d.",
		h.failMetrics.Len()+h.metrics.Len(),
		h.MetricBufferLimit,
		h.metrics.Total(),
		h.metrics.Drops()+h.failMetrics.Drops())

	var err error
	if !h.failMetrics.IsEmpty() {
		bufLen := h.failMetrics.Len()
		// how many batches of failed writes we need to write.
		nBatches := bufLen/h.MetricBatchSize + 1
		batchSize := h.MetricBatchSize

		for i := 0; i < nBatches; i++ {
			// If it's the last batch, only grab the metrics that have not had
			// a write attempt already (this is primarily to preserve order).
			if i == nBatches-1 {
				batchSize = bufLen % h.MetricBatchSize
			}
			batch := h.failMetrics.Batch(batchSize)
			// If we've already failed previous writes, don't bother trying to
			// write to this output again. We are not exiting the loop just so
			// that we can rotate the metrics to preserve order.
			if err == nil {
				err = h.write(batch)
			}
			if err != nil {
				h.failMetrics.Add(batch...)
			}
		}
	}

	batch := h.metrics.Batch(h.MetricBatchSize)
	// see comment above about not trying to write to an already failed output.
	// if h.failMetrics is empty then err will always be nil at this point.
	if err == nil {
		err = h.write(batch)
	}
	if err != nil {
		h.failMetrics.Add(batch...)
		return err
	}
	return nil
}

func (h *Handler) write(metrics []metric.Metric) error {
	if metrics == nil || len(metrics) == 0 {
		return nil
	}

	formattedMetrics := h.format(metrics)
	if len(formattedMetrics) == 0 {
		return nil
	}

	start := time.Now()
	payload := NewPayload(h.API.licenseKey, formattedMetrics)
	err := h.API.Write(payload)
	elapsed := time.Since(start)
	if err == nil {
		log.Infof("Write batch of %d metrics in %s\n",
			len(metrics), elapsed)
	}
	return err
}

func (h *Handler) format(metrics []metric.Metric) []interface{} {
	m := make([]interface{}, len(metrics))
	for i, metric := range metrics {
		m[i] = metric.Format()
	}
	return m
}
