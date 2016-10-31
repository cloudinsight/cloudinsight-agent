package agent

import (
	"testing"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
	"github.com/stretchr/testify/assert"
)

const checkInterval = time.Second

type testPlugin struct{}

func (p *testPlugin) Check(agg metric.Aggregator, instance plugin.Instance) error {
	agg.Add("gauge", metric.Metric{
		Name:  "test",
		Value: 10,
	})
	return nil
}

type testPanicPlugin struct{}

func (p *testPanicPlugin) Check(agg metric.Aggregator, instance plugin.Instance) error {
	panic("got panic!")
}

type testTimeoutPlugin struct{}

func (p *testTimeoutPlugin) Check(agg metric.Aggregator, instance plugin.Instance) error {
	agg.Add("gauge", metric.Metric{
		Name:  "test.timeout",
		Value: 20,
	})
	time.Sleep(2 * checkInterval)
	return nil
}

func TestCollectWithPanic(t *testing.T) {
	shutdown := make(chan struct{})
	metricC := make(chan metric.Metric, 5)
	defer close(metricC)

	rp := &plugin.RunningPlugin{
		Name:   "testPlugin",
		Plugin: &testPlugin{},
		Config: &plugin.Config{
			Instances: []plugin.Instance{map[string]interface{}{}},
		},
	}
	rpp := &plugin.RunningPlugin{
		Name:   "testPanicPlugin",
		Plugin: &testPanicPlugin{},
		Config: &plugin.Config{
			Instances: []plugin.Instance{map[string]interface{}{}},
		},
	}
	a := &Agent{
		conf: &config.Config{},
	}

	for _, p := range []*plugin.RunningPlugin{rp, rpp} {
		go func(rp *plugin.RunningPlugin) {
			err := a.collect(shutdown, rp, checkInterval, metricC)
			assert.NoError(t, err)
		}(p)
	}

	// Waiting for collect goroutines running.
	time.Sleep(200 * time.Millisecond)

	assert.Len(t, metricC, 1)
	testm := <-metricC
	assert.Equal(t, "test", testm.Name)
	assert.EqualValues(t, 10, testm.Value)
	close(shutdown)

	// Waiting for collect goroutines stopping.
	time.Sleep(time.Millisecond)
}

func TestCollectWithTimeout(t *testing.T) {
	shutdown := make(chan struct{})
	metricC := make(chan metric.Metric, 5)
	defer close(metricC)

	rp := &plugin.RunningPlugin{
		Name:   "testTimeoutPlugin",
		Plugin: &testTimeoutPlugin{},
		Config: &plugin.Config{
			Instances: []plugin.Instance{map[string]interface{}{}},
		},
	}
	a := &Agent{
		conf: &config.Config{},
	}

	go func() {
		err := a.collect(shutdown, rp, checkInterval, metricC)
		assert.NoError(t, err)
	}()

	// Waiting for collect goroutines timeout.
	time.Sleep(checkInterval)
	time.Sleep(200)

	assert.Len(t, metricC, 0)
	close(shutdown)

	// Waiting for collect goroutines stopping.
	time.Sleep(time.Millisecond)
}
