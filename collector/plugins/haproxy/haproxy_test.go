package haproxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common"
)

var hastats = `
# pxname,svname,qcur,qmax,scur,smax,slim,stot,bin,bout,dreq,dresp,ereq,econ,eresp,wretr,wredis,status,weight,act,bck,chkfail,chkdown,lastchg,downtime,qlimit,pid,iid,sid,throttle,lbtot,tracked,type,rate,rate_lim,rate_max,check_status,check_code,check_duration,hrsp_1xx,hrsp_2xx,hrsp_3xx,hrsp_4xx,hrsp_5xx,hrsp_other,hanafail,req_rate,req_rate_max,req_tot,cli_abrt,srv_abrt,comp_in,comp_out,comp_byp,comp_rsp,lastsess,last_chk,last_agt,qtime,ctime,rtime,ttime,
hastats,FRONTEND,,,1,1,64,2,3403,6697,0,0,0,,,,,OPEN,,,,,,,,,1,1,0,,,,0,0,0,1,,,,0,8,0,0,0,0,,1,1,9,,,0,0,0,0,,,,,,,,
hastats,BACKEND,0,0,0,0,7,0,3403,6697,0,0,,0,0,0,0,UP,0,0,0,,0,37,0,,1,1,0,,0,,1,0,,0,,,,0,0,0,0,0,0,,,,,0,0,0,0,0,0,0,,,0,0,0,30,
`

var hastats2 = `
# pxname,svname,qcur,qmax,scur,smax,slim,stot,bin,bout,dreq,dresp,ereq,econ,eresp,wretr,wredis,status,weight,act,bck,chkfail,chkdown,lastchg,downtime,qlimit,pid,iid,sid,throttle,lbtot,tracked,type,rate,rate_lim,rate_max,check_status,check_code,check_duration,hrsp_1xx,hrsp_2xx,hrsp_3xx,hrsp_4xx,hrsp_5xx,hrsp_other,hanafail,req_rate,req_rate_max,req_tot,cli_abrt,srv_abrt,comp_in,comp_out,comp_byp,comp_rsp,lastsess,last_chk,last_agt,qtime,ctime,rtime,ttime,
hastats,FRONTEND,,,1,2,64,201,59487,176047,0,0,0,,,,,OPEN,,,,,,,,,1,1,0,,,,0,1,0,2,,,,0,207,0,0,0,0,,1,2,208,,,0,0,0,0,,,,,,,,
hastats,BACKEND,0,0,0,0,7,0,59487,176047,0,0,,0,0,0,0,UP,0,0,0,,0,7327,0,,1,1,0,,0,,1,0,,0,,,,0,0,0,0,0,0,,,,,0,0,0,0,0,0,0,,,0,0,0,24,
`

func TestHAProxyCheck(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rsp string

		switch r.URL.Path {
		case "/stub_stats;csv;norefresh":
			rsp = hastats
		case "/stub_stats2;csv;norefresh":
			rsp = hastats2
		default:
			panic("Cannot handle request")
		}
		fmt.Fprintln(w, rsp)
	}))
	defer ts.Close()

	h := HAProxy{
		URL: fmt.Sprintf("%s/stub_stats", ts.URL),
	}
	h2 := HAProxy{
		URL: fmt.Sprintf("%s/stub_stats2", ts.URL),
	}
	server := ts.Listener.Addr().String()

	fields := map[string]float64{
		"haproxy.frontend.session.current": 1,
		"haproxy.frontend.session.limit":   64,
		"haproxy.frontend.requests.rate":   1,
		"haproxy.frontend.session.pct":     float64(1) / float64(64) * 100,
	}
	tags := []string{
		"server:" + server,
		"proxy:hastats",
		"type:FRONTEND",
	}
	testutil.AssertCheckWithMetrics(t, h.Check, 12, fields, tags)

	fields = map[string]float64{
		"haproxy.backend.queue.current":   0,
		"haproxy.backend.session.current": 0,
		"haproxy.backend.session.limit":   7,
		"haproxy.backend.queue.time":      0,
		"haproxy.backend.connect.time":    0,
		"haproxy.backend.response.time":   0,
		"haproxy.backend.session.time":    30,
		"haproxy.backend.session.pct":     0,
	}
	tags = []string{
		"server:" + server,
		"proxy:hastats",
		"type:BACKEND",
	}
	testutil.AssertCheckWithMetrics(t, h.Check, 12, fields, tags)

	fields = map[string]float64{
		"haproxy.frontend.session.rate":     199,
		"haproxy.frontend.bytes.in_rate":    56084,
		"haproxy.frontend.bytes.out_rate":   169350,
		"haproxy.frontend.denied.req_rate":  0,
		"haproxy.frontend.denied.resp_rate": 0,
		"haproxy.frontend.errors.req_rate":  0,
		"haproxy.frontend.response.1xx":     0,
		"haproxy.frontend.response.2xx":     199,
		"haproxy.frontend.response.3xx":     0,
		"haproxy.frontend.response.4xx":     0,
		"haproxy.frontend.response.5xx":     0,
		"haproxy.frontend.response.other":   0,
	}
	tags = []string{
		"server:" + server,
		"proxy:hastats",
		"type:FRONTEND",
	}
	testutil.AssertCheckWithRateMetrics(t, h.Check, h2.Check, 39, fields, tags)

	fields = map[string]float64{
		"haproxy.backend.session.rate":        0,
		"haproxy.backend.bytes.in_rate":       56084,
		"haproxy.backend.bytes.out_rate":      169350,
		"haproxy.backend.denied.req_rate":     0,
		"haproxy.backend.denied.resp_rate":    0,
		"haproxy.backend.errors.con_rate":     0,
		"haproxy.backend.errors.resp_rate":    0,
		"haproxy.backend.warnings.retr_rate":  0,
		"haproxy.backend.warnings.redis_rate": 0,
		"haproxy.backend.response.1xx":        0,
		"haproxy.backend.response.2xx":        0,
		"haproxy.backend.response.3xx":        0,
		"haproxy.backend.response.4xx":        0,
		"haproxy.backend.response.5xx":        0,
		"haproxy.backend.response.other":      0,
	}
	tags = []string{
		"server:" + server,
		"proxy:hastats",
		"type:BACKEND",
	}
	testutil.AssertCheckWithRateMetrics(t, h.Check, h2.Check, 39, fields, tags)
}
