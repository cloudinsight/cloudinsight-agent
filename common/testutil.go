package testutil

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/stretchr/testify/assert"
)

// MockAggregator XXX
func MockAggregator(
	metrics chan metric.Metric,
	conf *config.Config,
) metric.Aggregator {
	return metric.NewAggregator(metrics, 1, conf.GetHostname(), formatter, nil, nil, 0)
}

func formatter(m metric.Metric) interface{} {
	return nil
}

// AssertContainsMetricWithTags XXX
func AssertContainsMetricWithTags(
	t *testing.T,
	metrics []metric.Metric,
	name string,
	expectedValue float64,
	tags []string,
	delta ...float64,
) {
	var actualValue float64
	var deltaValue float64
	if len(delta) > 0 {
		deltaValue = delta[0]
	}
	for _, m := range metrics {
		if m.Name == name && reflect.DeepEqual(m.Tags, tags) {
			if value, ok := m.Value.(float64); ok {
				actualValue = value
				if (value >= expectedValue-deltaValue) && (value <= expectedValue+deltaValue) {
					// Found the point, return without failing
					return
				}
			} else {
				assert.Fail(t, fmt.Sprintf("Metric \"%s\" does not have type float64", name))
			}
		}
	}
	msg := fmt.Sprintf(
		"Could not find metric \"%s\" with requested tags %v of value %f, Actual: %f",
		name, tags, expectedValue, actualValue)
	assert.Fail(t, msg)
}
