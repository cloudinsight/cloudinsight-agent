package agent

import (
	"fmt"
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/stretchr/testify/assert"
)

func TestFormatter(t *testing.T) {
	m := metric.NewMetric("test.formatter", 99, []string{"test"})
	actual := fmt.Sprintf("%v", formatter(m))
	assert.Equal(t, actual, "[test.formatter 0 99 map[tags:[test]]]")
}
