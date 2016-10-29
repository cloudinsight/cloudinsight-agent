package statsd

import (
	"fmt"
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/stretchr/testify/assert"
)

func TestFormatter(t *testing.T) {
	m := metric.NewMetric("test.formatter", 99, []string{"test"})
	actual := fmt.Sprintf("%v", formatter(m))
	assert.Contains(t, actual, "metric:test.formatter")
	assert.Contains(t, actual, "points:[[0 99]]")
	assert.Contains(t, actual, "tags:[test]")
	assert.Contains(t, actual, "interval:30")
}
