package nginx

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common"
)

const nginxSampleResponse = `
Active connections: 585
server accepts handled requests
 85340 85340 35085
Reading: 4 Writing: 135 Waiting: 446
`
const nginxSampleResponse2 = `
Active connections: 585
server accepts handled requests
 85440 85430 35115
Reading: 4 Writing: 135 Waiting: 446
`
const tengineSampleResponse = `
Active connections: 403
server accepts handled requests request_time
 8853 8533 3502 1546565864
Reading: 8 Writing: 125 Waiting: 946
`

const tengineSampleResponse2 = `
Active connections: 403
server accepts handled requests request_time
 8893 8563 3512 1546565864
Reading: 8 Writing: 125 Waiting: 946
`

func TestNginxCheck(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rsp string

		switch r.URL.Path {
		case "/stub_status":
			rsp = nginxSampleResponse
		case "/stub_status2":
			rsp = nginxSampleResponse2
		case "/tengine_status":
			rsp = tengineSampleResponse
		case "/tengine_status2":
			rsp = tengineSampleResponse2
		default:
			panic("Cannot handle request")
		}

		fmt.Fprintln(w, rsp)
	}))
	defer ts.Close()

	n := &Nginx{
		NginxStatusURL: fmt.Sprintf("%s/stub_status", ts.URL),
		Tags:           []string{"service:nginx"},
	}
	n2 := &Nginx{
		NginxStatusURL: fmt.Sprintf("%s/stub_status2", ts.URL),
		Tags:           []string{"service:nginx"},
	}

	nt := &Nginx{
		NginxStatusURL: fmt.Sprintf("%s/tengine_status", ts.URL),
		Tags:           []string{"service:tengine"},
	}
	nt2 := &Nginx{
		NginxStatusURL: fmt.Sprintf("%s/tengine_status2", ts.URL),
		Tags:           []string{"service:tengine"},
	}

	nginxFields := map[string]float64{
		"nginx.net.connections":        585,
		"nginx.net.reading":            4,
		"nginx.net.writing":            135,
		"nginx.net.waiting":            446,
		"nginx.net.conn_opened_per_s":  100,
		"nginx.net.conn_dropped_per_s": 10,
		"nginx.net.request_per_s":      30,
	}
	testutil.AssertCheckWithRateMetrics(t, n.Check, n2.Check, 7, nginxFields, n.Tags)

	tengineFields := map[string]float64{
		"nginx.net.connections":        403,
		"nginx.net.reading":            8,
		"nginx.net.writing":            125,
		"nginx.net.waiting":            946,
		"nginx.net.conn_opened_per_s":  40,
		"nginx.net.conn_dropped_per_s": 10,
		"nginx.net.request_per_s":      10,
	}
	testutil.AssertCheckWithRateMetrics(t, nt.Check, nt2.Check, 7, tengineFields, nt.Tags)
}
