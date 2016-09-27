package metric

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/startover/cloudinsight-agent/common/log"
	"github.com/startover/cloudinsight-agent/common/util"
)

var (
	// DefaultHistogramAggregates XXX
	DefaultHistogramAggregates = []string{"max", "median", "avg", "count"}

	// DefaultHistogramPercentiles XXX
	DefaultHistogramPercentiles = []float64{0.95}
)

// Generator XXX
type Generator interface {
	Sample(value float64, timestamp int64)
	Flush(timestamp int64, interval float64) []Metric
	IsExpired(expirySeconds int64) bool
}

// Formatter XXX
type Formatter func(metric Metric) interface{}

// Context XXX
type Context [4]string

// NewGenerator XXX
func NewGenerator(metricType string, metric Metric, formatter Formatter) (Generator, error) {
	metric.Type = metricType
	metric.Formatter = formatter
	if metric.Samplerate == 0 {
		// If not set, we just set samplerate to 1 as default.
		metric.Samplerate = 1
	}

	switch metricType {
	case "gauge":
		return &Gauge{metric}, nil
	case "bucketgauge":
		return &BucketGauge{
			Gauge{metric},
		}, nil
	case "counter":
		return &Counter{metric}, nil
	case "rate":
		return &Rate{
			Metric: metric,
		}, nil
	case "count":
		return &Count{metric}, nil
	case "set":
		return &Set{
			Metric: metric,
			values: make(map[float64]bool),
		}, nil
	case "histogram":
		return &Histogram{
			Metric:      metric,
			aggregates:  DefaultHistogramAggregates,
			percentiles: DefaultHistogramPercentiles,
		}, nil
	}

	return nil, fmt.Errorf("unsupported metricType: %s", metricType)
}

// Metric XXX
type Metric struct {
	Name           string
	Value          interface{}
	Tags           []string
	Hostname       string
	DeviceName     string
	Timestamp      int64
	LastSampleTime int64
	Type           string
	Samplerate     float64
	Formatter      Formatter
}

// GetCorrectedValue XXX
func (m *Metric) GetCorrectedValue() (float64, error) {
	var value float64

	switch d := m.Value.(type) {
	case int:
		value = float64(int(d))
	case int32:
		value = float64(int32(d))
	case uint32:
		value = float64(uint32(d))
	case int64:
		value = float64(int64(d))
	case uint64:
		value = float64(uint64(d))
	case float32:
		value = float64(d)
	case float64:
		value = float64(d)
	default:
		return 0, fmt.Errorf("undeterminable type: %s", d)
	}

	if math.IsNaN(value) {
		return 0, fmt.Errorf("NaN is an unsupported value for %s", m.Name)
	}

	return value, nil
}

func (m *Metric) removeDuplicates(s []string) []string {
	result := []string{}
	found := map[string]bool{}
	for _, val := range s {
		if _, ok := found[val]; !ok {
			result = append(result, val)
			found[val] = true
		}
	}
	return result
}

// Context XXX
func (m *Metric) Context() Context {
	tags := m.removeDuplicates(m.Tags)
	sort.Strings(tags)
	return Context{m.Name, strings.Join(tags, ","), m.Hostname, m.DeviceName}
}

// IsExpired XXX
func (m *Metric) IsExpired(expirySeconds int64) bool {
	now := time.Now().Unix()
	if now-m.LastSampleTime > expirySeconds {
		return true
	}
	return false
}

// Format XXX
func (m Metric) Format() interface{} {
	return m.Formatter(m)
}

// Gauge XXX
type Gauge struct {
	Metric
}

// Sample XXX
func (g *Gauge) Sample(value float64, timestamp int64) {
	g.Value = value
	g.Timestamp = timestamp
	g.LastSampleTime = time.Now().Unix()
}

// Flush XXX
func (g *Gauge) Flush(timestamp int64, interval float64) []Metric {
	if g.Timestamp != 0 {
		return []Metric{g.Metric}
	}

	m := g.Metric
	m.Timestamp = timestamp
	return []Metric{m}
}

// BucketGauge XXX
type BucketGauge struct {
	Gauge
}

// Flush XXX
func (bg *BucketGauge) Flush(timestamp int64, interval float64) []Metric {
	m := bg.Metric
	m.Type = "gauge"
	m.Timestamp = timestamp
	return []Metric{m}
}

// Count XXX
type Count struct {
	Metric
}

// Sample XXX
func (c *Count) Sample(value float64, timestamp int64) {
	correctedValue, err := c.GetCorrectedValue()
	if err != nil {
		log.Error(err)
		return
	}

	c.Value = correctedValue + value
	c.LastSampleTime = time.Now().Unix()
}

// Flush XXX
func (c *Count) Flush(timestamp int64, interval float64) []Metric {
	m := c.Metric
	m.Timestamp = timestamp
	return []Metric{m}
}

// MonotonicCount XXX
type MonotonicCount struct {
	Metric

	preCounter float64
	curCounter float64
	count      float64
}

// Sample XXX
func (mc *MonotonicCount) Sample(value float64, timestamp int64) {
	mc.preCounter = mc.curCounter
	mc.curCounter = value
	mc.count += math.Max(0, mc.curCounter-mc.preCounter)
	mc.LastSampleTime = time.Now().Unix()
}

// Flush XXX
func (mc *MonotonicCount) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		mc.count = 0
	}()

	m := mc.Metric
	m.Value = mc.count
	m.Timestamp = timestamp
	return []Metric{m}
}

// Counter XXX
type Counter struct {
	Metric
}

// Sample XXX
func (ct *Counter) Sample(value float64, timestamp int64) {
	if ct.Samplerate == 0 {
		log.Error("The samplerate can not be zero.")
		return
	}

	correctedValue, err := ct.GetCorrectedValue()
	if err != nil {
		log.Error(err)
		return
	}

	ct.Value = correctedValue + value*float64(int(1/ct.Samplerate))
	ct.LastSampleTime = time.Now().Unix()
}

// Flush XXX
func (ct *Counter) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		ct.Value = 0
	}()

	correctedValue, err := ct.GetCorrectedValue()
	if err != nil {
		log.Error(err)
		return []Metric{}
	}

	m := ct.Metric
	m.Value = correctedValue / interval
	m.Timestamp = timestamp
	return []Metric{m}
}

// Histogram XXX
type Histogram struct {
	Metric

	count       int64
	samples     []float64
	aggregates  []string
	percentiles []float64
}

// Sample XXX
func (h *Histogram) Sample(value float64, timestamp int64) {
	if h.Samplerate == 0 {
		log.Error("The samplerate can not be zero.")
		return
	}
	h.count += int64(1 / h.Samplerate)
	h.samples = append(h.samples, value)
	h.LastSampleTime = time.Now().Unix()
}

// Flush XXX
func (h *Histogram) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		h.count = 0
		h.samples = []float64{}
	}()

	if h.count == 0 {
		return []Metric{}
	}

	sort.Float64s(h.samples)
	length := len(h.samples)
	var median float64
	if length == 1 {
		median = h.samples[0]
	} else {
		median = h.samples[util.Cast(float64(length/2-1))]
	}

	aggregators := map[string]float64{
		"min":    h.samples[0],
		"max":    h.samples[length-1],
		"median": median,
		"avg":    util.Sum(h.samples) / float64(length),
		"count":  float64(h.count) / interval,
	}

	metrics := []Metric{}
	for _, suffix := range h.aggregates {
		if value, ok := aggregators[suffix]; ok {
			m := h.Metric
			m.Name = fmt.Sprintf("%s.%s", m.Name, suffix)
			m.Value = value
			m.Timestamp = timestamp
			if suffix == "count" {
				m.Type = "rate"
			} else {
				m.Type = "gauge"
			}
			metrics = append(metrics, m)
		}
	}

	for _, p := range h.percentiles {
		m := h.Metric
		m.Name = fmt.Sprintf("%s.%spercentile", m.Name, strconv.Itoa(int(p*float64(100))))
		m.Value = h.samples[util.Cast(p*float64(length)-1)]
		m.Timestamp = timestamp
		m.Type = "gauge"
		metrics = append(metrics, m)
	}

	return metrics
}

// Set XXX
type Set struct {
	Metric

	values map[float64]bool
}

// Sample XXX
func (s *Set) Sample(value float64, timestamp int64) {
	if s.values == nil {
		s.values = make(map[float64]bool)
	}

	if _, ok := s.values[value]; !ok {
		s.values[value] = true
	}
	s.Metric.LastSampleTime = time.Now().Unix()
}

// Flush XXX
func (s *Set) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		s.values = nil
	}()

	m := s.Metric
	m.Value = float64(len(s.values))
	m.Timestamp = timestamp
	return []Metric{m}
}

// Rate XXX
type Rate struct {
	Metric

	preSample [2]float64
	curSample [2]float64
}

// Sample XXX
func (r *Rate) Sample(value float64, timestamp int64) {
	ts := time.Now().Unix()
	r.preSample = r.curSample
	r.curSample = [2]float64{float64(ts), value}
	r.Metric.LastSampleTime = ts
}

// Flush XXX
func (r *Rate) Flush(timestamp int64, interval float64) []Metric {
	if r.preSample[0] == 0 {
		return nil
	}

	timeDuration := r.curSample[0] - r.preSample[0]
	if timeDuration == 0 {
		log.Warnf("Metric %s has an duration of 0. Not flushing.", r.Metric.Name)
		return nil
	}

	delta := r.curSample[1] - r.preSample[1]
	if delta < 0 {
		log.Infof("Metric %s has a rate < 0. Counter may have been Reset.", r.Metric.Name)
		return nil
	}

	m := r.Metric
	m.Value = delta / timeDuration
	m.Timestamp = timestamp
	return []Metric{m}
}
