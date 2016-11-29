package agent

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
	"github.com/stretchr/testify/assert"
)

const checkInterval = time.Second

type testPlugin struct {
	Tags []string
}

func (p *testPlugin) Check(agg metric.Aggregator) error {
	agg.Add("gauge", metric.Metric{
		Name:  "test",
		Value: 10,
		Tags:  p.Tags,
	})
	return nil
}

type testPanicPlugin struct{}

func (p *testPanicPlugin) Check(agg metric.Aggregator) error {
	panic("got panic!")
}

type testTimeoutPlugin struct{}

func (p *testTimeoutPlugin) Check(agg metric.Aggregator) error {
	agg.Add("gauge", metric.Metric{
		Name:  "test.timeout",
		Value: 20,
	})
	time.Sleep(checkInterval)
	return nil
}

func TestCollectWithPanic(t *testing.T) {
	shutdown := make(chan struct{})
	metricC := make(chan metric.Metric, 5)
	defer close(metricC)

	rp := &plugin.RunningPlugin{
		Name:    "testPlugin",
		Plugins: []plugin.Plugin{&testPlugin{}},
	}
	rpp := &plugin.RunningPlugin{
		Name:    "testPanicPlugin",
		Plugins: []plugin.Plugin{&testPanicPlugin{}},
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

func TestCollectWithMultiInstances(t *testing.T) {
	shutdown := make(chan struct{})
	metricC := make(chan metric.Metric, 5)
	defer close(metricC)

	rp := &plugin.RunningPlugin{
		Name: "testPlugin",
		Plugins: []plugin.Plugin{
			&testPlugin{[]string{"instance:foo"}},
			&testPlugin{[]string{"instance:bar"}},
		},
	}

	a := &Agent{
		conf: &config.Config{},
	}

	go func() {
		err := a.collect(shutdown, rp, checkInterval, metricC)
		assert.NoError(t, err)
	}()

	// Waiting for collect goroutines running.
	time.Sleep(200 * time.Millisecond)

	assert.Len(t, metricC, 2)
	testm := <-metricC
	assert.Equal(t, "test", testm.Name)
	assert.EqualValues(t, 10, testm.Value)
	assert.Equal(t, []string{"instance:foo"}, testm.Tags)

	testm = <-metricC
	assert.Equal(t, "test", testm.Name)
	assert.EqualValues(t, 10, testm.Value)
	assert.Equal(t, []string{"instance:bar"}, testm.Tags)
	close(shutdown)

	// Waiting for collect goroutines stopping.
	time.Sleep(time.Millisecond)
}

func TestCollectWithTimeout(t *testing.T) {
	shutdown := make(chan struct{})
	metricC := make(chan metric.Metric, 5)
	defer close(metricC)

	rp := &plugin.RunningPlugin{
		Name:    "testTimeoutPlugin",
		Plugins: []plugin.Plugin{&testTimeoutPlugin{}},
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

func TestRun(t *testing.T) {
	shutdown := make(chan struct{})
	conf := config.Config{}
	a := NewAgent(&conf)
	done := make(chan bool)

	go func() {
		err := a.Run(shutdown)
		assert.NoError(t, err)
		done <- true
	}()

	close(shutdown)
	<-done
}

func init() {
	log.SetOutput(ioutil.Discard)
}
