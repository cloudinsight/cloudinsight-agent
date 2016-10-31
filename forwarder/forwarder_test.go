package forwarder

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/api"
	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/stretchr/testify/assert"
)

func TestMetricHandler(t *testing.T) {
	conf := config.Config{}
	f := NewForwarder(&conf)

	ts := httptest.NewServer(http.HandlerFunc(f.metricHandler))
	defer ts.Close()

	tsRemote := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		assert.Equal(t, "/infrastructure/metrics", req.URL.Path)
		assert.Equal(t, "POST", req.Method)
	}))
	defer tsRemote.Close()

	f.api = api.NewAPI(
		tsRemote.URL,
		"dummy-key",
		5*time.Second,
	)

	resp, err := http.Get(ts.URL)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestRun(t *testing.T) {
	shutdown := make(chan struct{})
	conf := config.Config{
		GlobalConfig: config.GlobalConfig{
			BindHost:   "127.0.0.1",
			ListenPort: 9999,
		},
	}

	go func() {
		f := NewForwarder(&conf)

		tsRemote := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
			assert.Equal(t, "/infrastructure/metrics", req.URL.Path)
			assert.Equal(t, "POST", req.Method)
		}))
		defer tsRemote.Close()

		f.api = api.NewAPI(
			tsRemote.URL,
			"dummy-key",
			5*time.Second,
		)
		err := f.Run(shutdown)
		assert.NoError(t, err)
	}()

	// Waiting for the forwarder server running.
	time.Sleep(time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:9999/infrastructure/metrics")
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestShutdown(t *testing.T) {
	if os.Getenv("BE_SHUTDOWN") == "1" {
		shutdown := make(chan struct{})
		conf := config.Config{
			GlobalConfig: config.GlobalConfig{
				BindHost:   "127.0.0.1",
				ListenPort: 1234,
			},
		}

		go func() {
			f := NewForwarder(&conf)
			_ = f.Run(shutdown)
		}()

		// Waiting for the forwarder server running.
		time.Sleep(time.Millisecond)

		close(shutdown)

		// Waiting for the forwarder server stopping.
		time.Sleep(time.Millisecond)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestShutdown")
	cmd.Env = append(os.Environ(), "BE_SHUTDOWN=1")
	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return
	}
	t.Fatalf("process ran with err %v, want exit status 1", err)
}
