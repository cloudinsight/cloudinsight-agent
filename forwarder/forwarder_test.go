package forwarder

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/api"
	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/stretchr/testify/assert"
)

var (
	fakeURL        = "http://test.cloudinsight.com"
	fakeLicenseKey = "123456"
)

func fakeAPI() *api.API {
	return api.NewAPI(fakeURL, fakeLicenseKey, 10*time.Second)
}

func TestMetricHandler(t *testing.T) {
	conf := config.Config{
		GlobalConfig: config.GlobalConfig{
			CiURL:      "https://dc-cloud.oneapm.com",
			BindHost:   "127.0.0.1",
			ListenPort: 10010,
			StatsdPort: 8251,
		},
	}

	f := NewForwarder(&conf)
	f.api = fakeAPI()
	server := httptest.NewServer(http.HandlerFunc(f.metricHandler))
	defer server.Close()

	resp, err := http.Get(server.URL)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}
