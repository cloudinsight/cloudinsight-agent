package agent

import (
	"strings"
	"time"

	"github.com/startover/cloudinsight-agent/common/log"
	"github.com/startover/cloudinsight-agent/common/metric"
	"github.com/startover/cloudinsight-agent/common/plugin"
)

// NewAggregator XXX
func NewAggregator(
	pluginConfig *plugin.Config,
	metrics chan metric.Metric,
) metric.Aggregator {
	agg := MetricAggregator{}
	agg.metrics = metrics
	agg.context = make(map[metric.Context]metric.Generator)
	agg.pluginConfig = pluginConfig
	return &agg
}

// MetricAggregator XXX
type MetricAggregator struct {
	metrics      chan metric.Metric
	context      map[metric.Context]metric.Generator
	pluginConfig *plugin.Config
	interval     float64
}

// AddMetrics XXX
func (ma *MetricAggregator) AddMetrics(
	metricType string,
	prefix string,
	fields map[string]interface{},
	tags []string,
	deviceName string,
	t ...int64,
) {
	if len(prefix) == 0 || len(fields) == 0 {
		return
	}

	var timestamp int64
	if len(t) > 0 {
		timestamp = t[0]
	} else {
		timestamp = time.Now().Unix()
	}

	for name, value := range fields {
		ma.Add(metricType, metric.Metric{
			Name:       strings.Join([]string{prefix, name}, "."),
			Value:      value,
			Tags:       tags,
			DeviceName: deviceName,
			Timestamp:  timestamp,
		})
	}
}

// Add XXX
func (ma *MetricAggregator) Add(metricType string, m metric.Metric) {
	generator, ok := ma.context[m.Context()]
	if !ok {
		var err error
		generator, err = metric.NewGenerator(metricType, m, ma.Format)
		// if metricType == "rate" {
		// log.Infof("New generator for %v", m)
		// log.Infof("Context: %s", m.Context())
		// }
		if err != nil {
			log.Errorf("Error adding metric [%v]: %s\n", m, err.Error())
			return
		}
		ma.context[m.Context()] = generator
	}

	value, err := m.GetCorrectedValue()
	if err != nil {
		log.Error(err)
		return
	}

	// log.Infoln(generator)
	generator.Sample(value, 1, m.Timestamp)
}

// Flush XXX
func (ma *MetricAggregator) Flush() {
	timestamp := time.Now().Unix()
	for _, metric := range ma.context {
		metrics := metric.Flush(timestamp, ma.interval)
		for _, m := range metrics {
			ma.metrics <- m
		}
	}
}

// Format metrics coming from the MetricsAggregator. Will look like:
// (metric, timestamp, value, {"tags": ["tag1", "tag2"], ...})
func (ma *MetricAggregator) Format(m metric.Metric) interface{} {
	var ret []interface{}
	ret = append(ret, m.Name)
	ret = append(ret, m.Timestamp)
	ret = append(ret, m.Value)

	attributes := make(map[string]interface{})
	if len(m.Tags) > 0 {
		attributes["tags"] = m.Tags
	}
	if m.Hostname != "" {
		attributes["hostname"] = m.Hostname
	}
	if m.DeviceName != "" {
		attributes["device_name"] = m.DeviceName
	}
	if m.Type != "" {
		attributes["type"] = m.Type
	}
	if len(attributes) > 0 {
		ret = append(ret, attributes)
	}

	return ret
}
