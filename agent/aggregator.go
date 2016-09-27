package agent

import (
	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/metric"
)

// NewAggregator XXX
func NewAggregator(
	metrics chan metric.Metric,
	conf *config.Config,
) metric.Aggregator {
	return metric.NewAggregator(metrics, 1, conf.GetHostname(), formatter)
}

// Format metrics coming from the MetricsAggregator. Will look like:
// (metric, timestamp, value, {"tags": ["tag1", "tag2"], ...})
func formatter(m metric.Metric) interface{} {
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
