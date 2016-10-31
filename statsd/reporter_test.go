package statsd

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/api"
	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/stretchr/testify/assert"
)

func TestPost(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		assert.Equal(t, "/infrastructure/metrics", req.URL.Path)
		assert.Equal(t, "POST", req.Method)
	}))
	defer ts.Close()

	conf := config.Config{
		GlobalConfig: config.GlobalConfig{
			BindHost:   "127.0.0.1",
			StatsdPort: 1234,
		},
	}

	r := NewReporter(&conf)
	r.api = api.NewAPI(
		ts.URL,
		"dummy-key",
		5*time.Second,
	)

	err := r.Post(nil)
	assert.NoError(t, err)
}
