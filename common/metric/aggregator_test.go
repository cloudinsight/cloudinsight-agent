package metric

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type MetricSorter []Metric

func (m MetricSorter) Len() int      { return len(m) }
func (m MetricSorter) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m MetricSorter) Less(i, j int) bool {
	if m[i].Name == m[j].Name {
		return getValue(m[i]) < getValue(m[j])
	}
	return m[i].Name < m[j].Name
}

func getValue(m Metric) float64 {
	value, _ := m.getCorrectedValue()
	return value
}

func TestAddMetrics(t *testing.T) {
	a := aggregator{
		metrics: make(chan Metric, 10),
		context: make(map[Context]Generator),
	}
	defer close(a.metrics)

	fields := map[string]interface{}{
		"usage": 99,
	}
	a.AddMetrics("gauge", "", fields, nil, "")
	assert.Len(t, a.context, 0)

	a.AddMetrics("gauge", "agg.test", fields, nil, "")
	expected := Metric{
		Name:  "agg.test.usage",
		Value: 99,
	}
	assert.Contains(t, a.context, expected.context())
	assert.Len(t, a.context, 1)

	now := time.Now().UnixNano()
	a.AddMetrics("gauge", "agg.test", fields, nil, "", now)
	expected = Metric{
		Name:      "agg.test.usage",
		Value:     99,
		Timestamp: now,
	}
	assert.Contains(t, a.context, expected.context())
	assert.Len(t, a.context, 1)

	a.AddMetrics("gauge", "agg.test", fields, []string{"agg:test"}, "")
	expected = Metric{
		Name:  "agg.test.usage",
		Value: 99,
		Tags:  []string{"agg:test"},
	}
	assert.Contains(t, a.context, expected.context())
	assert.Len(t, a.context, 2)

	a.AddMetrics("gauge", "agg.test", fields, []string{"agg:test"}, "agg")
	expected = Metric{
		Name:       "agg.test.usage",
		Value:      99,
		Tags:       []string{"agg:test"},
		DeviceName: "agg",
	}
	assert.Contains(t, a.context, expected.context())
	assert.Len(t, a.context, 3)
}

func TestAdd(t *testing.T) {
	a := aggregator{
		metrics: make(chan Metric, 10),
		context: make(map[Context]Generator),
	}
	defer close(a.metrics)

	testm := NewMetric("agg.test", 1)
	a.Add("gauge", testm)
	assert.Contains(t, a.context, testm.context())
	assert.Len(t, a.context, 1)

	testm = NewMetric("agg.test", 2, []string{"agg:test"})
	a.Add("gauge", testm)
	assert.Contains(t, a.context, testm.context())
	assert.Len(t, a.context, 2)

	a.Add("gauge", testm)
	assert.Len(t, a.context, 2)
}

func TestFlush(t *testing.T) {
	a := aggregator{
		metrics: make(chan Metric, 10),
		context: make(map[Context]Generator),
	}
	defer close(a.metrics)

	a.Add("gauge", NewMetric("agg.test", 1))
	a.Add("gauge", NewMetric("agg.test", 2, []string{"agg:test"}))
	a.Flush()
	assert.Len(t, a.context, 2)
	assert.Len(t, a.metrics, 2)

	time.Sleep(1 * time.Second)
	a.Flush()
	assert.Len(t, a.context, 0)
	assert.Len(t, a.metrics, 2)

	metrics := make([]Metric, 2)
	for i := 0; i < 2; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	testm := metrics[0]
	assert.Equal(t, "agg.test", testm.Name)
	assert.Equal(t, float64(1), getValue(testm))
	assert.Nil(t, testm.Tags)

	testm = metrics[1]
	assert.Equal(t, "agg.test", testm.Name)
	assert.Equal(t, float64(2), getValue(testm))
	assert.Equal(t, []string{"agg:test"}, testm.Tags)

	now := time.Now().UnixNano()
	a.Add("gauge", Metric{
		Name:      "agg.test",
		Value:     3,
		Tags:      []string{"agg:test"},
		Timestamp: now,
	})
	a.Flush()
	testm = <-a.metrics
	assert.Equal(t, "agg.test", testm.Name)
	assert.Equal(t, float64(3), getValue(testm))
	assert.Equal(t, []string{"agg:test"}, testm.Tags)
	assert.Equal(t, now, testm.Timestamp)
}

func TestCounterNormalization(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 10,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.SubmitPackets("int:1|c")
	a.SubmitPackets("int:4|c")
	a.SubmitPackets("int:15|c")
	a.SubmitPackets("float:5|c")
	a.Flush()

	assert.Len(t, a.metrics, 2)

	metrics := make([]Metric, 2)
	for i := 0; i < 2; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	testm := metrics[0]
	assert.Equal(t, "float", testm.Name)
	assert.Equal(t, float64(0.5), getValue(testm))
	assert.Equal(t, "myhost", testm.Hostname)

	testm = metrics[1]
	assert.Equal(t, "int", testm.Name)
	assert.Equal(t, float64(2), getValue(testm))
	assert.Equal(t, "myhost", testm.Hostname)
}

func TestHistogramNormalization(t *testing.T) {
	a := aggregator{
		metrics:             make(chan Metric, 20),
		context:             make(map[Context]Generator),
		interval:            10,
		hostname:            "myhost",
		histogramAggregates: append(DefaultHistogramAggregates, "min"),
	}
	defer close(a.metrics)

	for i := 0; i < 5; i++ {
		a.SubmitPackets("h1:1|h")
	}

	for i := 0; i < 20; i++ {
		a.SubmitPackets("h2:1|h")
	}
	a.Flush()
	assert.Len(t, a.metrics, 12)

	metrics := make([]Metric, 12)
	for i := 0; i < 12; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	testm := metrics[2]
	assert.Equal(t, float64(0.5), getValue(testm))

	testm = metrics[8]
	assert.Equal(t, float64(2), getValue(testm))
}

func TestTags(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.SubmitPackets("gauge:1|c")
	a.SubmitPackets("gauge:2|c|@1")
	a.SubmitPackets("gauge:4|c|#tag1,tag2")
	a.SubmitPackets("gauge:8|c|#tag2,tag1")
	a.SubmitPackets("gauge:16|c|#tag3,tag4")
	a.Flush()
	assert.Len(t, a.metrics, 3)

	metrics := make([]Metric, 3)
	for i := 0; i < 3; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	testm := metrics[0]
	assert.Equal(t, "gauge", testm.Name)
	assert.Equal(t, float64(3), getValue(testm))
	assert.Nil(t, testm.Tags)
	assert.Equal(t, "myhost", testm.Hostname)

	testm = metrics[1]
	assert.Equal(t, "gauge", testm.Name)
	assert.Equal(t, float64(12), getValue(testm))
	assert.Equal(t, []string{"tag1", "tag2"}, testm.Tags)
	assert.Equal(t, "myhost", testm.Hostname)

	testm = metrics[2]
	assert.Equal(t, "gauge", testm.Name)
	assert.Equal(t, float64(16), getValue(testm))
	assert.Equal(t, []string{"tag3", "tag4"}, testm.Tags)
	assert.Equal(t, "myhost", testm.Hostname)
}

func TestMagicTags(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.SubmitPackets("my.gauge.a:1|c|#host:test-a")
	a.SubmitPackets("my.gauge.b:4|c|#tag1,tag2,host:test-b")
	a.SubmitPackets("my.gauge.b:8|c|#host:test-b,tag2,tag1")
	a.SubmitPackets("my.gauge.c:10|c|#tag3")
	a.SubmitPackets("my.gauge.c:16|c|#device:floppy,tag3")
	a.Flush()
	assert.Len(t, a.metrics, 4)

	metrics := make([]Metric, 4)
	for i := 0; i < 4; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	testm := metrics[0]
	assert.Equal(t, "my.gauge.a", testm.Name)
	assert.Equal(t, float64(1), getValue(testm))
	assert.Nil(t, testm.Tags)
	assert.Equal(t, "test-a", testm.Hostname)

	testm = metrics[1]
	assert.Equal(t, "my.gauge.b", testm.Name)
	assert.Equal(t, float64(12), getValue(testm))
	assert.Equal(t, []string{"tag1", "tag2"}, testm.Tags)
	assert.Equal(t, "test-b", testm.Hostname)

	testm = metrics[2]
	assert.Equal(t, "my.gauge.c", testm.Name)
	assert.Equal(t, float64(10), getValue(testm))
	assert.Equal(t, []string{"tag3"}, testm.Tags)
	assert.Empty(t, testm.DeviceName)

	testm = metrics[3]
	assert.Equal(t, "my.gauge.c", testm.Name)
	assert.Equal(t, float64(16), getValue(testm))
	assert.Equal(t, []string{"tag3"}, testm.Tags)
	assert.Equal(t, "floppy", testm.DeviceName)
}

func TestCounter(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.SubmitPackets("my.first.counter:1|c")
	a.SubmitPackets("my.first.counter:5|c")
	a.SubmitPackets("my.second.counter:1|c")
	a.SubmitPackets("my.third.counter:3|c")
	a.Flush()
	assert.Len(t, a.metrics, 3)

	metrics := make([]Metric, 3)
	for i := 0; i < 3; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	testm := metrics[0]
	assert.Equal(t, "my.first.counter", testm.Name)
	assert.Equal(t, float64(6), getValue(testm))
	assert.Equal(t, "myhost", testm.Hostname)

	testm = metrics[1]
	assert.Equal(t, "my.second.counter", testm.Name)
	assert.Equal(t, float64(1), getValue(testm))

	testm = metrics[2]
	assert.Equal(t, "my.third.counter", testm.Name)
	assert.Equal(t, float64(3), getValue(testm))

	a.Flush()
	assert.Len(t, a.metrics, 3)
	for i := 0; i < 3; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	testm = metrics[0]
	assert.Equal(t, "my.first.counter", testm.Name)
	assert.Equal(t, float64(0), getValue(testm))

	testm = metrics[1]
	assert.Equal(t, "my.second.counter", testm.Name)
	assert.Equal(t, float64(0), getValue(testm))

	testm = metrics[2]
	assert.Equal(t, "my.third.counter", testm.Name)
	assert.Equal(t, float64(0), getValue(testm))
}

func TestSampledCounter(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.SubmitPackets("sampled.counter:1|c|@0.5")
	a.Flush()
	assert.Len(t, a.metrics, 1)

	testm := <-a.metrics
	assert.Equal(t, "sampled.counter", testm.Name)
	assert.Equal(t, float64(2), getValue(testm))
}

func TestGauge(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.SubmitPackets("my.first.gauge:1|g")
	a.SubmitPackets("my.first.gauge:5|g")
	a.SubmitPackets("my.second.gauge:1.5|g")
	a.Flush()
	assert.Len(t, a.metrics, 2)

	metrics := make([]Metric, 2)
	for i := 0; i < 2; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	testm := metrics[0]
	assert.Equal(t, "my.first.gauge", testm.Name)
	assert.Equal(t, float64(5), getValue(testm))
	assert.Equal(t, "myhost", testm.Hostname)

	testm = metrics[1]
	assert.Equal(t, "my.second.gauge", testm.Name)
	assert.Equal(t, float64(1.5), getValue(testm))

	// Ensure that old gauges get dropped due to old timestamps
	a.Add("gauge", NewMetric("my.first.gauge", 5))
	a.Add("gauge", Metric{
		Name:      "my.first.gauge",
		Value:     1,
		Timestamp: 1000000000,
	})
	a.Add("gauge", Metric{
		Name:      "my.second.gauge",
		Value:     20,
		Timestamp: 1000000000,
	})
	a.Flush()
	assert.Len(t, a.metrics, 1)

	testm = <-a.metrics
	assert.Equal(t, "my.first.gauge", testm.Name)
	assert.Equal(t, float64(5), getValue(testm))
	assert.Equal(t, "myhost", testm.Hostname)
}

func TestSampledGauge(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.SubmitPackets("sampled.gauge:10|g|@0.1")
	a.Flush()
	assert.Len(t, a.metrics, 1)

	testm := <-a.metrics
	assert.Equal(t, "sampled.gauge", testm.Name)
	assert.Equal(t, float64(10), getValue(testm))
}

func TestSets(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.SubmitPackets("my.set:10|s")
	a.SubmitPackets("my.set:20|s")
	a.SubmitPackets("my.set:20|s")
	a.SubmitPackets("my.set:30|s")
	a.SubmitPackets("my.set:30|s")
	a.SubmitPackets("my.set:30|s")
	a.Flush()
	assert.Len(t, a.metrics, 1)

	testm := <-a.metrics
	assert.Equal(t, "my.set", testm.Name)
	assert.Equal(t, float64(3), getValue(testm))

	// Assert there are no more sets
	a.Flush()
	assert.Len(t, a.metrics, 0)
}

func TestStringSets(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.SubmitPackets("my.set:string|s")
	a.SubmitPackets("my.set:sets|s")
	a.SubmitPackets("my.set:sets|s")
	a.SubmitPackets("my.set:test|s")
	a.SubmitPackets("my.set:test|s")
	a.SubmitPackets("my.set:test|s")
	a.Flush()
	assert.Len(t, a.metrics, 1)

	testm := <-a.metrics
	assert.Equal(t, "my.set", testm.Name)
	assert.Equal(t, float64(3), getValue(testm))

	// Assert there are no more sets
	a.Flush()
	assert.Len(t, a.metrics, 0)
}

func TestRate(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.Add("rate", NewMetric("my.rate", 10))
	time.Sleep(1 * time.Second)
	a.Add("rate", NewMetric("my.rate", 40))
	a.Flush()
	assert.Len(t, a.metrics, 1)

	// Check that the rate is calculated correctly
	testm := <-a.metrics
	assert.Equal(t, "my.rate", testm.Name)
	assert.Equal(t, float64(30), getValue(testm))

	// Assert that no more rates are given
	a.Flush()
	assert.Len(t, a.metrics, 0)
}

func TestRateErrors(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.Add("rate", NewMetric("my.rate", 10))
	time.Sleep(1 * time.Second)
	a.Add("rate", NewMetric("my.rate", 9))
	a.Flush()
	// Since the difference < 0 we shouldn't get a value
	assert.Len(t, a.metrics, 0)

	a.Add("rate", NewMetric("my.rate", 10))
	a.Add("rate", NewMetric("my.rate", 40))
	a.Flush()
	assert.Len(t, a.metrics, 0)
}

func TestHistogram(t *testing.T) {
	a := aggregator{
		metrics:             make(chan Metric, 10),
		context:             make(map[Context]Generator),
		interval:            1,
		hostname:            "myhost",
		histogramAggregates: append(DefaultHistogramAggregates, "min"),
	}
	defer close(a.metrics)

	percentiles := rand.Perm(100)
	for i := range percentiles {
		for j := 0; j < 20; j++ {
			for _, mType := range []string{"h", "ms"} {
				m := fmt.Sprintf("my.p:%d|%s", i+1, mType)
				a.SubmitPackets(m)
			}
		}
	}
	a.Flush()
	assert.Len(t, a.metrics, 6)

	metrics := make([]Metric, 6)
	for i := 0; i < 6; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	testm := metrics[0]
	assert.Equal(t, "my.p.95percentile", testm.Name)
	assert.Equal(t, float64(95), getValue(testm))
	assert.Equal(t, "myhost", testm.Hostname)

	testm = metrics[1]
	assert.Equal(t, "my.p.avg", testm.Name)
	assert.Equal(t, float64(50.5), getValue(testm))

	testm = metrics[2]
	assert.Equal(t, "my.p.count", testm.Name)
	assert.Equal(t, float64(4000), getValue(testm))

	testm = metrics[3]
	assert.Equal(t, "my.p.max", testm.Name)
	assert.Equal(t, float64(100), getValue(testm))

	testm = metrics[4]
	assert.Equal(t, "my.p.median", testm.Name)
	assert.Equal(t, float64(50), getValue(testm))

	testm = metrics[5]
	assert.Equal(t, "my.p.min", testm.Name)
	assert.Equal(t, float64(1), getValue(testm))

	// Ensure that histograms are reset.
	a.Flush()
	assert.Len(t, a.metrics, 0)
}

func TestSampledHistogram(t *testing.T) {
	a := aggregator{
		metrics:             make(chan Metric, 10),
		context:             make(map[Context]Generator),
		interval:            1,
		hostname:            "myhost",
		histogramAggregates: append(DefaultHistogramAggregates, "min"),
	}
	defer close(a.metrics)

	a.SubmitPackets("sampled.hist:5|h|@0.5")
	a.Flush()
	assert.Len(t, a.metrics, 6)

	metrics := make([]Metric, 6)
	for i := 0; i < 6; i++ {
		metrics[i] = <-a.metrics
	}

	for _, m := range metrics {
		if m.Name == "sampled.hist.count" {
			assert.Equal(t, float64(2), getValue(m))
		} else {
			assert.Equal(t, float64(5), getValue(m))
		}
	}
}

func TestBatchSubmission(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	packets := []string{
		"counter:1|c",
		"counter:1|c",
		"gauge:1|g",
	}
	packet := strings.Join(packets, "\n")
	a.SubmitPackets(packet)
	a.Flush()
	assert.Len(t, a.metrics, 2)

	metrics := make([]Metric, 2)
	for i := 0; i < 2; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	testm := metrics[0]
	assert.Equal(t, "counter", testm.Name)
	assert.Equal(t, float64(2), getValue(testm))

	testm = metrics[1]
	assert.Equal(t, "gauge", testm.Name)
	assert.Equal(t, float64(1), getValue(testm))
}

func TestMonkeyBatchingWithoutTags(t *testing.T) {
	a := aggregator{
		metrics:             make(chan Metric, 10),
		context:             make(map[Context]Generator),
		interval:            1,
		hostname:            "myhost",
		histogramAggregates: append(DefaultHistogramAggregates, "min"),
	}
	defer close(a.metrics)

	a.SubmitPackets("test_hist:0.3|ms:2.5|ms|@0.5:3|ms")
	a.Flush()
	assert.Len(t, a.metrics, 6)

	metrics := make([]Metric, 6)
	for i := 0; i < 6; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	aRef := aggregator{
		metrics:             make(chan Metric, 10),
		context:             make(map[Context]Generator),
		interval:            1,
		hostname:            "myhost",
		histogramAggregates: append(DefaultHistogramAggregates, "min"),
	}
	defer close(aRef.metrics)

	packets := []string{
		"test_hist:0.3|ms",
		"test_hist:2.5|ms|@0.5",
		"test_hist:3|ms",
	}
	packet := strings.Join(packets, "\n")
	aRef.SubmitPackets(packet)
	aRef.Flush()
	assert.Len(t, aRef.metrics, 6)

	metricsRef := make([]Metric, 6)
	for i := 0; i < 6; i++ {
		metricsRef[i] = <-aRef.metrics
	}
	sort.Sort(MetricSorter(metricsRef))

	for i := 0; i < 6; i++ {
		assert.Equal(t, getValue(metrics[i]), getValue(metricsRef[i]))
	}
}

func TestMonkeyBatchingWithTags(t *testing.T) {
	a := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(a.metrics)

	a.SubmitPackets("test_gauge:1.5|g|#tag1:one,tag2:two:2.3|g|#tag3:three:3|g")
	a.Flush()
	assert.Len(t, a.metrics, 3)

	metrics := make([]Metric, 3)
	for i := 0; i < 3; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	aRef := aggregator{
		metrics:  make(chan Metric, 10),
		context:  make(map[Context]Generator),
		interval: 1,
		hostname: "myhost",
	}
	defer close(aRef.metrics)

	packets := []string{
		"test_gauge:1.5|g|#tag1:one,tag2:two",
		"test_gauge:2.3|g|#tag3:three",
		"test_gauge:3|g",
	}
	packet := strings.Join(packets, "\n")
	aRef.SubmitPackets(packet)
	aRef.Flush()
	assert.Len(t, aRef.metrics, 3)

	metricsRef := make([]Metric, 3)
	for i := 0; i < 3; i++ {
		metricsRef[i] = <-aRef.metrics
	}
	sort.Sort(MetricSorter(metricsRef))

	for i := 0; i < 3; i++ {
		assert.Equal(t, getValue(metrics[i]), getValue(metricsRef[i]))
		assert.Equal(t, metrics[i].Tags, metricsRef[i].Tags)
	}
}

func TestMonkeyBatchingWithTagsAndSamplerate(t *testing.T) {
	a := aggregator{
		metrics:             make(chan Metric, 10),
		context:             make(map[Context]Generator),
		interval:            1,
		hostname:            "myhost",
		histogramAggregates: append(DefaultHistogramAggregates, "min"),
	}
	defer close(a.metrics)

	a.SubmitPackets("test_metric:1.5|c|#tag1:one,tag2:two:2.3|g|#tag3:three:3|g:42|h|#tag1:12,tag42:42|@0.22")
	a.Flush()
	assert.Len(t, a.metrics, 9)

	metrics := make([]Metric, 9)
	for i := 0; i < 9; i++ {
		metrics[i] = <-a.metrics
	}
	sort.Sort(MetricSorter(metrics))

	aRef := aggregator{
		metrics:             make(chan Metric, 10),
		context:             make(map[Context]Generator),
		interval:            1,
		hostname:            "myhost",
		histogramAggregates: append(DefaultHistogramAggregates, "min"),
	}
	defer close(aRef.metrics)

	packets := []string{
		"test_metric:1.5|c|#tag1:one,tag2:two",
		"test_metric:2.3|g|#tag3:three",
		"test_metric:3|g",
		"test_metric:42|h|#tag1:12,tag42:42|@0.22",
	}
	packet := strings.Join(packets, "\n")
	aRef.SubmitPackets(packet)
	aRef.Flush()
	assert.Len(t, aRef.metrics, 9)

	metricsRef := make([]Metric, 9)
	for i := 0; i < 9; i++ {
		metricsRef[i] = <-aRef.metrics
	}
	sort.Sort(MetricSorter(metricsRef))

	for i := 0; i < 9; i++ {
		assert.Equal(t, getValue(metrics[i]), getValue(metricsRef[i]))
		assert.Equal(t, metrics[i].Tags, metricsRef[i].Tags)
	}
}

func TestInvalidPackets(t *testing.T) {
	invalidPackets := []string{
		"missing.value.and.type",
		"missing.type:2",
		"missing.value|c",
		"2|c",
		"unknown.type:2|z",
		"string.value:abc|c",
		"string.sample.rate:0|c|@abc",
	}

	for _, packet := range invalidPackets {
		_, err := parsePacket(packet)
		if err == nil {
			t.Errorf("Parsing packet %s should have resulted in an error\n", packet)
		}
	}
}
