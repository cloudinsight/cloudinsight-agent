package forwarder

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/api"
	"github.com/cloudinsight/cloudinsight-agent/common/config"
)

var (
	fakeURL        = "http://test.cloudinsight.com"
	fakeLicenseKey = "123456"
)

func fakeAPI() *api.API {
	return api.NewAPI(fakeURL, fakeLicenseKey, 10*time.Second)
}

func TestMetricHandler(t *testing.T) {
	f := NewForwarder(&config.DefaultConfig)
	f.api = fakeAPI()
	server := httptest.NewServer(http.HandlerFunc(f.metricHandler))
	defer server.Close()

	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("Received non-200 response: %d\n", resp.StatusCode)
	}
}
