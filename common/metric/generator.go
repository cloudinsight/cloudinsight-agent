package metric

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/util"
)

var (
	// DefaultHistogramAggregates XXX
	DefaultHistogramAggregates = []string{"max", "median", "avg", "count"}

	// DefaultHistogramPercentiles XXX
	DefaultHistogramPercentiles = []float64{0.95}
)

// Generator generates metrics
type Generator interface {
	Sample(value float64, timestamp int64)
	Flush(timestamp int64, interval float64) []Metric
	IsExpired(timestamp, expirySeconds int64) bool
}

// NewGenerator creates a new instance of Generator(gauge, bucketGauge, counter, rate, count, set, histogram).
func NewGenerator(metricType string, metric Metric, formatter Formatter, histogramAggregates []string, histogramPercentiles []float64) (Generator, error) {
	metric.Type = metricType
	metric.Formatter = formatter
	if metric.Samplerate == 0 {
		// If not set, we just set samplerate to 1 as default.
		metric.Samplerate = 1
	}
	if histogramAggregates == nil {
		histogramAggregates = DefaultHistogramAggregates
	}
	if histogramPercentiles == nil {
		histogramPercentiles = DefaultHistogramPercentiles
	}

	switch metricType {
	case "gauge":
		return &gauge{metric}, nil
	case "bucketgauge":
		return &bucketGauge{
			gauge{metric},
		}, nil
	case "counter":
		return &counter{
			Metric: metric,
		}, nil
	case "rate":
		return &rate{
			Metric: metric,
		}, nil
	case "count":
		return &count{
			Metric: metric,
		}, nil
	case "monotoniccount":
		return &monotonicCount{
			Metric: metric,
		}, nil
	case "set":
		return &set{
			Metric: metric,
			values: make(map[float64]bool),
		}, nil
	case "histogram":
		return &histogram{
			Metric:      metric,
			aggregates:  histogramAggregates,
			percentiles: histogramPercentiles,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported metricType: %s", metricType)
	}
}

type gauge struct {
	Metric
}

func (g *gauge) Sample(value float64, timestamp int64) {
	g.Value = value
	g.Timestamp = timestamp
	g.LastSampleTime = time.Now().Unix()
}

func (g *gauge) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		g.Value = nil
	}()

	if g.Value == nil {
		return nil
	}

	if g.Timestamp != 0 {
		return []Metric{g.Metric}
	}

	m := g.Metric
	m.Timestamp = timestamp
	return []Metric{m}
}

type bucketGauge struct {
	gauge
}

func (bg *bucketGauge) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		bg.Value = nil
	}()

	if bg.Value == nil {
		return nil
	}

	m := bg.Metric
	m.Type = "gauge"
	m.Timestamp = timestamp
	return []Metric{m}
}

type count struct {
	Metric

	hasSampled bool
}

func (c *count) Sample(value float64, timestamp int64) {
	if c.hasSampled {
		correctedValue, err := c.getCorrectedValue()
		if err != nil {
			log.Error(err)
			return
		}

		c.Value = correctedValue + value
	} else {
		c.Value = value * float64(int(1/c.Samplerate))
		c.hasSampled = true
	}

	c.LastSampleTime = time.Now().Unix()
}

func (c *count) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		c.hasSampled = false
		c.Value = nil
	}()

	if c.Value == nil {
		return nil
	}

	m := c.Metric
	m.Timestamp = timestamp
	return []Metric{m}
}

type monotonicCount struct {
	Metric

	preCounter  float64
	curCounter  float64
	count       float64
	hasSampled  bool
	sampleCount int
}

func (mc *monotonicCount) Sample(value float64, timestamp int64) {
	mc.sampleCount++
	if mc.hasSampled {
		mc.preCounter = mc.curCounter
	}
	mc.curCounter = value
	if mc.hasSampled {
		mc.count += math.Max(0, mc.curCounter-mc.preCounter)
	}
	mc.LastSampleTime = time.Now().Unix()
	mc.hasSampled = true
}

func (mc *monotonicCount) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		mc.count = 0
	}()

	if mc.sampleCount == 1 {
		return nil
	}

	m := mc.Metric
	m.Value = mc.count
	m.Timestamp = timestamp
	return []Metric{m}
}

type counter struct {
	Metric

	hasSampled bool
}

func (ct *counter) Sample(value float64, timestamp int64) {
	if ct.Samplerate == 0 {
		log.Error("The samplerate can not be zero.")
		return
	}

	if ct.hasSampled {
		correctedValue, err := ct.getCorrectedValue()
		if err != nil {
			log.Error(err)
			return
		}

		ct.Value = correctedValue + value*float64(int(1/ct.Samplerate))
	} else {
		ct.Value = value * float64(int(1/ct.Samplerate))
		ct.hasSampled = true
	}

	ct.LastSampleTime = time.Now().Unix()
}

func (ct *counter) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		ct.Value = 0
	}()

	correctedValue, err := ct.getCorrectedValue()
	if err != nil {
		log.Error(err)
		return nil
	}

	m := ct.Metric
	m.Value = correctedValue / interval
	m.Timestamp = timestamp
	return []Metric{m}
}

type histogram struct {
	Metric

	count       int64
	samples     []float64
	aggregates  []string
	percentiles []float64
}

func (h *histogram) Sample(value float64, timestamp int64) {
	if h.Samplerate == 0 {
		log.Error("The samplerate can not be zero.")
		return
	}
	h.count += int64(1 / h.Samplerate)
	h.samples = append(h.samples, value)
	h.LastSampleTime = time.Now().Unix()
}

func (h *histogram) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		h.count = 0
		h.samples = nil
	}()

	if h.count == 0 {
		return nil
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

type set struct {
	Metric

	values map[float64]bool
}

func (s *set) Sample(value float64, timestamp int64) {
	if s.values == nil {
		s.values = make(map[float64]bool)
	}

	if _, ok := s.values[value]; !ok {
		s.values[value] = true
	}
	s.LastSampleTime = time.Now().Unix()
}

func (s *set) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		s.values = nil
	}()

	if s.values == nil {
		return nil
	}

	m := s.Metric
	m.Value = float64(len(s.values))
	m.Timestamp = timestamp
	return []Metric{m}
}

type rate struct {
	Metric

	preSample [2]float64
	curSample [2]float64
}

func (r *rate) Sample(value float64, timestamp int64) {
	ts := time.Now().Unix()
	r.preSample = r.curSample
	r.curSample = [2]float64{float64(ts), value}
	r.LastSampleTime = ts
}

func (r *rate) Flush(timestamp int64, interval float64) []Metric {
	defer func() {
		r.preSample[0] = 0
	}()

	if r.preSample[0] == 0 {
		return nil
	}

	timeDuration := r.curSample[0] - r.preSample[0]
	if timeDuration == 0 {
		log.Warnf("Metric %s has an duration of 0. Not flushing.", r.Name)
		return nil
	}

	delta := r.curSample[1] - r.preSample[1]
	if delta < 0 {
		log.Infof("Metric %s has a rate < 0. Counter may have been Reset.", r.Name)
		return nil
	}

	m := r.Metric
	m.Value = delta / timeDuration
	m.Timestamp = timestamp
	return []Metric{m}
}
