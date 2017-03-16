package phpfpm

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common"
)

var pfStatus = `
pool:                 www
process manager:      dynamic
start time:           16/Nov/2016:10:10:07 +0800
start since:          28225
accepted conn:        3
listen queue:         0
max listen queue:     0
listen queue len:     0
idle processes:       1
active processes:     1
total processes:      2
max active processes: 1
max children reached: 0
slow requests:        0
`

var pfStatus2 = `
pool:                 www
process manager:      dynamic
start time:           16/Nov/2016:10:10:07 +0800
start since:          32803
accepted conn:        7
listen queue:         0
max listen queue:     0
listen queue len:     0
idle processes:       1
active processes:     1
total processes:      2
max active processes: 1
max children reached: 0
slow requests:        0
`

func TestPHPFPMCheck(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rsp string

		switch r.URL.Path {
		case "/stub_status":
			rsp = pfStatus
		case "/stub_status2":
			rsp = pfStatus2
		default:
			panic("Cannot handle request")
		}
		fmt.Fprintln(w, rsp)
	}))
	defer ts.Close()

	pf := PHPFPM{
		StatusURL: fmt.Sprintf("%s/stub_status", ts.URL),
		Tags:      []string{"service:phpfpm"},
	}
	pf2 := PHPFPM{
		StatusURL: fmt.Sprintf("%s/stub_status2", ts.URL),
		Tags:      []string{"service:phpfpm"},
	}

	fields := map[string]float64{
		"php_fpm.listen_queue.size":     0,
		"php_fpm.listen_queue.max_size": 0,
		"php_fpm.processes.idle":        1,
		"php_fpm.processes.active":      1,
		"php_fpm.processes.total":       2,
		"php_fpm.processes.max_active":  1,
	}
	tags := []string{
		"pool:www",
		"service:phpfpm",
	}
	testutil.AssertCheckWithMetrics(t, pf.Check, 6, fields, tags)

	fields = map[string]float64{
		"php_fpm.requests.accepted":     4,
		"php_fpm.processes.max_reached": 0,
		"php_fpm.requests.slow":         0,
	}
	tags = []string{
		"pool:www",
		"service:phpfpm",
	}
	testutil.AssertCheckWithRateMetrics(t, pf.Check, pf2.Check, 9, fields, tags)
}
