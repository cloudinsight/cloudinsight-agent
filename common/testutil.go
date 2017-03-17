package testutil

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Checker XXX
type Checker func(agg metric.Aggregator) error

// MockAggregator XXX
func MockAggregator(
	metrics chan metric.Metric,
) metric.Aggregator {
	conf := &config.Config{}
	return metric.NewAggregator(metrics, 1, conf.GetHostname(), formatter, nil, nil, 0)
}

func formatter(m metric.Metric) interface{} {
	return nil
}

// AssertCheckWithMetrics XXX
func AssertCheckWithMetrics(
	t *testing.T,
	checker Checker,
	expectedMetrics int,
	fields map[string]float64,
	tags []string,
	delta ...float64,
) {
	metricC := make(chan metric.Metric, 1000)
	defer close(metricC)
	agg := MockAggregator(metricC)

	err := checker(agg)
	require.NoError(t, err)
	agg.Flush()
	require.Len(t, metricC, expectedMetrics)

	metrics := make([]metric.Metric, expectedMetrics)
	for i := 0; i < expectedMetrics; i++ {
		metrics[i] = <-metricC
	}

	for name, value := range fields {
		AssertContainsMetricWithTags(t, metrics, name, value, tags, delta...)
	}
}

// AssertCheckWithRateMetrics XXX
func AssertCheckWithRateMetrics(
	t *testing.T,
	checker Checker,
	checker2 Checker,
	expectedMetrics int,
	fields map[string]float64,
	tags []string,
	delta ...float64,
) {
	metricC := make(chan metric.Metric, 1000)
	defer close(metricC)
	agg := MockAggregator(metricC)

	err := checker(agg)
	require.NoError(t, err)

	// Wait a second for collecting rate metrics.
	time.Sleep(time.Second)

	err = checker2(agg)
	require.NoError(t, err)
	agg.Flush()
	require.Len(t, metricC, expectedMetrics)

	metrics := make([]metric.Metric, expectedMetrics)
	for i := 0; i < expectedMetrics; i++ {
		metrics[i] = <-metricC
	}

	for name, value := range fields {
		AssertContainsMetricWithTags(t, metrics, name, value, tags, delta...)
	}
}

// AssertCheckWithLen XXX
func AssertCheckWithLen(
	t *testing.T,
	checker Checker,
	expectedMetrics int,
) {
	metricC := make(chan metric.Metric, 1000)
	defer close(metricC)
	agg := MockAggregator(metricC)

	err := checker(agg)
	require.NoError(t, err)
	agg.Flush()
	require.Len(t, metricC, expectedMetrics)
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
	var actualValue, deltaValue float64
	var actualTags []string
	if len(delta) > 0 {
		deltaValue = delta[0]
	}
	for _, m := range metrics {
		if m.Name == name {
			actualTags = m.Tags
			if value, ok := m.Value.(float64); ok {
				actualValue = value
			} else {
				assert.Fail(t, fmt.Sprintf("Metric \"%s\" does not have type float64", name))
			}
			if reflect.DeepEqual(actualTags, tags) {
				if (actualValue >= expectedValue-deltaValue) && (actualValue <= expectedValue+deltaValue) {
					// Found the point, return without failing
					return
				}
			}
		}
	}
	msg := fmt.Sprintf(
		"Could not find metric \"%s\" with requested tags %v of value %f, ActualValue: %f, ActualTags: %v",
		name, tags, expectedValue, actualValue, actualTags)
	assert.Fail(t, msg)
}
