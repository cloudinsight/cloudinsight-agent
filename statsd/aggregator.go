package statsd

import (
	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
)

const interval = 30

// NewAggregator XXX
func NewAggregator(
	metrics chan metric.Metric,
	conf *config.Config,
) metric.Aggregator {
	return metric.NewAggregator(metrics, interval, conf.GetHostname(), formatter, nil, nil, 0)
}

// Format metrics coming from the Aggregator. Will look like:
// {"metric": "a.b.c", "points": [[1474867457, 2]], "tags": ["tag1", "tag2"], "host": "xxx", "device_name": "xxx", "type": "gauge", "interval": 10}
func formatter(m metric.Metric) interface{} {
	return map[string]interface{}{
		"metric":      m.Name,
		"points":      [1]interface{}{[2]interface{}{m.Timestamp, m.Value}},
		"tags":        m.Tags,
		"host":        m.Hostname,
		"device_name": m.DeviceName,
		"type":        m.Type,
		"interval":    interval,
	}
}
