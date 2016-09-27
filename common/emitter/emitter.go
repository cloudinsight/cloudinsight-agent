package emitter

import (
	"reflect"
	"sync"
	"time"

	"github.com/startover/cloudinsight-agent/common/log"
	"github.com/startover/cloudinsight-agent/common/metric"
)

const (
	// DefaultMetricBatchSize is default size of metrics batch size.
	DefaultMetricBatchSize = 2000

	// DefaultMetricBufferLimit is default number of metrics kept. It should be a multiple of batch size.
	DefaultMetricBufferLimit = 10000

	// FlushLoggingInitial XXX
	FlushLoggingInitial = 5

	// FlushLoggingPeriod XXX
	FlushLoggingPeriod = 20
)

// Emitter contains the output configuration
type Emitter struct {
	Parent interface{}

	name      string
	emitCount int

	metrics           *Buffer
	failMetrics       *Buffer
	MetricBufferLimit int
	MetricBatchSize   int
}

// NewEmitter XXX
func NewEmitter(name string) *Emitter {
	bufferLimit := DefaultMetricBufferLimit
	batchSize := DefaultMetricBatchSize

	c := &Emitter{
		name:              name,
		metrics:           NewBuffer(batchSize),
		failMetrics:       NewBuffer(bufferLimit),
		MetricBufferLimit: bufferLimit,
		MetricBatchSize:   batchSize,
	}
	return c
}

// Run monitors the metrics channel and emits on the flush interval
func (e *Emitter) Run(shutdown chan struct{}, metricC chan metric.Metric, interval time.Duration) error {
	// Inelegant, but this sleep is to allow the collect threads to run, so that
	// the emitter will emit after metrics are flushed.
	time.Sleep(200 * time.Millisecond)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-shutdown:
			log.Infoln("Hang on, emitting any cached metrics before shutdown")
			e.emit()
			return nil
		case <-ticker.C:
			e.emit()
		case m := <-metricC:
			e.addMetric(m)
		}
	}
}

// emit sends the collected metrics to forwarder API
func (e *Emitter) emit() {
	e.emitCount++
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := e.flush()
		if err != nil {
			log.Infof("Error occured when posting to Forwarder API: %s\n", err.Error())
		}
	}()

	wg.Wait()
}

// AddMetric adds a metric to the Collector. It will post metrics to Forwarder
// when the metrics size has reached the MetricBatchSize.
func (e *Emitter) addMetric(metric metric.Metric) {
	e.metrics.Add(metric)
	if e.metrics.Len() == e.MetricBatchSize {
		batch := e.metrics.Batch(e.MetricBatchSize)
		err := e.Post(batch)
		if err != nil {
			e.failMetrics.Add(batch...)
		}
	}
}

// flush posts all cached metrics to Forwarder.
func (e *Emitter) flush() error {
	if e.shouldLog() {
		log.Infof("%s Flushing #%d. Buffer fullness: %d / %d metrics. "+
			"Total gathered metrics: %d. Total dropped metrics: %d.",
			e.name,
			e.emitCount,
			e.failMetrics.Len()+e.metrics.Len(),
			e.MetricBufferLimit,
			e.metrics.Total(),
			e.metrics.Drops()+e.failMetrics.Drops())
	} else {
		log.Debugf("%s Flushing #%d. Buffer fullness: %d / %d metrics. "+
			"Total gathered metrics: %d. Total dropped metrics: %d.",
			e.name,
			e.emitCount,
			e.failMetrics.Len()+e.metrics.Len(),
			e.MetricBufferLimit,
			e.metrics.Total(),
			e.metrics.Drops()+e.failMetrics.Drops())
	}

	if e.emitCount == FlushLoggingInitial {
		log.Infof("First flushes done, next flushes will be logged every %d flushes.", FlushLoggingPeriod)
	}

	var err error
	if !e.failMetrics.IsEmpty() {
		bufLen := e.failMetrics.Len()
		// how many batches of failed metrics we need to post.
		nBatches := bufLen/e.MetricBatchSize + 1
		batchSize := e.MetricBatchSize

		for i := 0; i < nBatches; i++ {
			// If it's the last batch, only grab the metrics that have not had
			// a post attempt already (this is primarily to preserve order).
			if i == nBatches-1 {
				batchSize = bufLen % e.MetricBatchSize
			}
			batch := e.failMetrics.Batch(batchSize)
			// If we've already failed previous Emit, don't bother trying to
			// post to Forwarder again. We are not exiting the loop just so
			// that we can rotate the metrics to preserve order.
			if err == nil {
				err = e.Post(batch)
			}
			if err != nil {
				e.failMetrics.Add(batch...)
			}
		}
	}

	batch := e.metrics.Batch(e.MetricBatchSize)
	// if c.failMetrics is empty then err will always be nil at this point.
	if err == nil {
		err = e.Post(batch)
	}
	if err != nil {
		e.failMetrics.Add(batch...)
		return err
	}
	return nil
}

// Post XXX
func (e *Emitter) Post(metrics []metric.Metric) error {
	if metrics == nil || len(metrics) == 0 {
		return nil
	}

	formattedMetrics := e.format(metrics)
	if len(formattedMetrics) == 0 {
		return nil
	}

	v := reflect.ValueOf(e.Parent)
	method := v.MethodByName("Post")
	if !method.IsValid() {
		log.Fatal("Can't find valid post method.")
	}

	ret := method.Call([]reflect.Value{reflect.ValueOf(formattedMetrics)})
	val := ret[0].Interface()
	if val != nil {
		if err, ok := val.(error); ok {
			return err
		}
	}
	return nil
}

// IsFirstRun XXX
func (e *Emitter) IsFirstRun() bool {
	return e.emitCount <= 1
}

func (e *Emitter) shouldLog() bool {
	return e.emitCount <= FlushLoggingInitial || e.emitCount%FlushLoggingPeriod == 0
}

func (e *Emitter) format(metrics []metric.Metric) []interface{} {
	m := make([]interface{}, len(metrics))
	for i, metric := range metrics {
		m[i] = metric.Format()
	}
	return m
}
