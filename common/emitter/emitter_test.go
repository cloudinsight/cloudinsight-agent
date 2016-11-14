package emitter

import (
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var first5 = []metric.Metric{
	metric.NewMetric("metric1", 101),
	metric.NewMetric("metric2", 101),
	metric.NewMetric("metric3", 101),
	metric.NewMetric("metric4", 101),
	metric.NewMetric("metric5", 101),
}

var next5 = []metric.Metric{
	metric.NewMetric("metric6", 101),
	metric.NewMetric("metric7", 101),
	metric.NewMetric("metric8", 101),
	metric.NewMetric("metric9", 101),
	metric.NewMetric("metric10", 101),
}

// Benchmark posting metrics.
func BenchmarkPost(b *testing.B) {
	p := &perfEmitter{
		Emitter: NewEmitter("Test"),
	}
	p.Emitter.Parent = p

	for n := 0; n < b.N; n++ {
		p.addMetric(first5[0])
		p.emit()
	}
}

// Benchmark posting metrics.
func BenchmarkPostEvery100(b *testing.B) {
	p := &perfEmitter{
		Emitter: NewEmitter("Test"),
	}
	p.Emitter.Parent = p

	for n := 0; n < b.N; n++ {
		p.addMetric(first5[0])
		if n%100 == 0 {
			p.emit()
		}
	}
}

// Benchmark adding metrics.
func BenchmarkAddFailPosts(b *testing.B) {
	p := &perfEmitter{
		Emitter:  NewEmitter("Test"),
		failPost: true,
	}
	p.Emitter.Parent = p

	for n := 0; n < b.N; n++ {
		p.addMetric(first5[0])
	}
}

// Test that we can post metrics with simple default setup.
func TestAddMetric(t *testing.T) {
	m := &mockEmitter{
		Emitter: NewEmitter("Test"),
	}
	m.Emitter.Parent = m

	for _, metric := range first5 {
		m.addMetric(metric)
	}
	for _, metric := range next5 {
		m.addMetric(metric)
	}
	assert.Len(t, m.Metrics(), 0)

	err := m.flush()
	assert.NoError(t, err)
	assert.Len(t, m.Metrics(), 10)

	m.emitCount = 6
	err = m.flush()
	assert.NoError(t, err)
}

// Test that the emitter doesn't flush until it's full.
func TestFlushWhenFull(t *testing.T) {
	m := &mockEmitter{
		Emitter: NewEmitter("Test"),
	}
	m.MetricBatchSize = 6
	m.MetricBufferLimit = 10
	m.Emitter.Parent = m

	// Fill buffer to 1 under limit
	for _, metric := range first5 {
		m.addMetric(metric)
	}
	// no flush yet
	assert.Len(t, m.Metrics(), 0)

	// add one more metric
	m.addMetric(next5[0])
	// now it flushed
	assert.Len(t, m.Metrics(), 6)

	// add one more metric and flush it manually
	m.addMetric(next5[1])
	err := m.flush()
	assert.NoError(t, err)
	assert.Len(t, m.Metrics(), 7)
}

// Test that running output doesn't flush until it's full.
func TestMultiFlushWhenFull(t *testing.T) {
	m := &mockEmitter{
		Emitter: NewEmitter("Test"),
	}
	m.MetricBatchSize = 4
	m.MetricBufferLimit = 12
	m.Emitter.Parent = m

	// Fill buffer past limit twelve
	for _, metric := range first5 {
		m.addMetric(metric)
	}
	for _, metric := range next5 {
		m.addMetric(metric)
	}
	// flushed twice
	assert.Len(t, m.Metrics(), 8)
}

func TestPostFail(t *testing.T) {
	m := &mockEmitter{
		Emitter:  NewEmitter("Test"),
		failPost: true,
	}
	m.MetricBatchSize = 4
	m.MetricBufferLimit = 12
	m.Emitter.Parent = m

	// Fill buffer to limit twice
	for _, metric := range first5 {
		m.addMetric(metric)
	}
	for _, metric := range next5 {
		m.addMetric(metric)
	}
	// no successful flush yet
	assert.Len(t, m.Metrics(), 0)

	// manual post fails
	err := m.flush()
	require.Error(t, err)
	// no successful flush yet
	assert.Len(t, m.Metrics(), 0)

	m.failPost = false
	err = m.flush()
	require.NoError(t, err)

	assert.Len(t, m.Metrics(), 10)
}

func TestRun(t *testing.T) {
	shutdown := make(chan struct{})
	metricC := make(chan metric.Metric, 5)
	defer close(metricC)
	interval := 200 * time.Millisecond
	done := make(chan bool)

	go func() {
		m := &mockEmitter{
			Emitter: NewEmitter("Test"),
		}
		m.Emitter.Parent = m
		err := m.Run(shutdown, metricC, interval)
		assert.NoError(t, err)
		done <- true
	}()

	// Waiting for the emitter timeout.
	time.Sleep(500 * time.Millisecond)

	close(shutdown)
	<-done
	return
}

type mockEmitter struct {
	*Emitter
	sync.Mutex

	metrics []interface{}

	// if true, mock a post failure
	failPost bool
}

func (m *mockEmitter) Post(metrics []interface{}) error {
	m.Lock()
	defer m.Unlock()
	if m.failPost {
		return fmt.Errorf("Failed Post!")
	}

	if m.metrics == nil {
		m.metrics = []interface{}{}
	}

	for _, metric := range metrics {
		m.metrics = append(m.metrics, metric)
	}
	return nil
}

func (m *mockEmitter) Metrics() []interface{} {
	m.Lock()
	defer m.Unlock()
	return m.metrics
}

type perfEmitter struct {
	*Emitter

	// if true, mock a post failure
	failPost bool
}

func (m *perfEmitter) Post(metrics []interface{}) error {
	if m.failPost {
		return fmt.Errorf("Failed Post!")
	}
	return nil
}

func init() {
	log.SetOutput(ioutil.Discard)
}
