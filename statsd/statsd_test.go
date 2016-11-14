package statsd

import (
	"io/ioutil"
	"net"
	"testing"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/stretchr/testify/assert"
)

func TestRun(t *testing.T) {
	shutdown := make(chan struct{})
	conf := config.Config{
		GlobalConfig: config.GlobalConfig{
			BindHost:   "127.0.0.1",
			StatsdPort: 1234,
		},
	}
	s := NewStatsd(&conf)
	done := make(chan bool)

	go func() {
		err := s.Run(shutdown)
		assert.NoError(t, err)
		done <- true
	}()

	close(shutdown)
	<-done
}

func TestUDPListen(t *testing.T) {
	shutdown := make(chan struct{})
	conf := config.Config{
		GlobalConfig: config.GlobalConfig{
			BindHost:   "127.0.0.1",
			StatsdPort: 1234,
		},
	}
	s := NewStatsd(&conf)
	defer close(s.in)

	go func() {
		err := s.listen(shutdown)
		assert.NoError(t, err)
	}()

	// Waiting for goroutine running.
	time.Sleep(200 * time.Millisecond)

	err := sendPacket("127.0.0.1:1234", "my.first.gauge:1|g")
	assert.NoError(t, err)
	packet := <-s.in
	assert.Equal(t, "my.first.gauge:1|g", string(packet))

	// Waiting for the goroutine stopping.
	close(shutdown)
	time.Sleep(time.Millisecond)
}

func TestParser(t *testing.T) {
	shutdown := make(chan struct{})
	conf := config.Config{
		GlobalConfig: config.GlobalConfig{
			BindHost:   "127.0.0.1",
			StatsdPort: 1234,
		},
	}
	interval := 200 * time.Millisecond
	metricC := make(chan metric.Metric, 5)
	s := NewStatsd(&conf)
	defer close(metricC)
	defer close(s.in)

	go func() {
		err := s.parser(shutdown, metricC, interval)
		assert.NoError(t, err)
	}()

	// Waiting for goroutine running.
	time.Sleep(200 * time.Millisecond)

	s.in <- []byte("statsd.parser.test:1|g")
	testm := <-metricC
	assert.Equal(t, "statsd.parser.test", testm.Name)
	assert.EqualValues(t, 1, testm.Value)
}

func sendPacket(addr string, packet string) error {
	udpClient, err := net.DialTimeout("udp", addr, time.Second)
	defer func() {
		_ = udpClient.Close()
	}()
	if err != nil {
		return err
	}
	_, err = udpClient.Write([]byte(packet))
	return err
}

func init() {
	log.SetOutput(ioutil.Discard)
}
