package haproxy

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/collector"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
)

// NewHAProxy XXX
func NewHAProxy(conf plugin.InitConfig) plugin.Plugin {
	return &HAProxy{}
}

// HAProxy XXX
type HAProxy struct {
	URL      string
	Username string
	Password string
}

var (
	// GAUGES XXX
	GAUGES = map[string]string{
		"qcur":     "queue.current",
		"scur":     "session.current",
		"slim":     "session.limit",
		"spct":     "session.pct",   // Calculated as: (scur/slim)*100
		"req_rate": "requests.rate", // HA Proxy 1.4 and higher
		"qtime":    "queue.time",    // HA Proxy 1.5 and higher
		"ctime":    "connect.time",  // HA Proxy 1.5 and higher
		"rtime":    "response.time", // HA Proxy 1.5 and higher
		"ttime":    "session.time",  // HA Proxy 1.5 and higher
	}

	// RATES XXX
	RATES = map[string]string{
		"stot":       "session.rate",
		"bin":        "bytes.in_rate",
		"bout":       "bytes.out_rate",
		"dreq":       "denied.req_rate",
		"dresp":      "denied.resp_rate",
		"ereq":       "errors.req_rate",
		"econ":       "errors.con_rate",
		"eresp":      "errors.resp_rate",
		"wretr":      "warnings.retr_rate",
		"wredis":     "warnings.redis_rate",
		"hrsp_1xx":   "response.1xx",   // HA Proxy 1.4 and higher
		"hrsp_2xx":   "response.2xx",   // HA Proxy 1.4 and higher
		"hrsp_3xx":   "response.3xx",   // HA Proxy 1.4 and higher
		"hrsp_4xx":   "response.4xx",   // HA Proxy 1.4 and higher
		"hrsp_5xx":   "response.5xx",   // HA Proxy 1.4 and higher
		"hrsp_other": "response.other", // HA Proxy 1.4 and higher
	}
)

var tr = &http.Transport{
	ResponseHeaderTimeout: time.Duration(3 * time.Second),
}

var client = &http.Client{
	Transport: tr,
	Timeout:   time.Duration(4 * time.Second),
}

// Check XXX
func (h *HAProxy) Check(agg metric.Aggregator) error {
	requestURI := h.URL
	if !strings.HasSuffix(h.URL, ";csv;norefresh") {
		requestURI += ";csv;norefresh"
	}

	u, err := url.Parse(h.URL)
	if err != nil {
		return fmt.Errorf("Unable parse server address '%s': %s", h.URL, err)
	}
	req, err := http.NewRequest("GET", requestURI, nil)
	if err != nil {
		return err
	}
	if h.Username != "" {
		req.SetBasicAuth(h.Username, h.Password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Request failed. Status: %s, URI: %s", resp.Status, requestURI)
	}

	return h.collectHAStats(agg, resp.Body, u.Host)
}

func (h *HAProxy) collectHAStats(agg metric.Aggregator, statsBody io.Reader, host string) error {
	reader := csv.NewReader(statsBody)
	result, err := reader.ReadAll()
	if err != nil {
		return err
	}
	var fields []string

	for i, row := range result {
		if i == 0 {
			row[0] = row[0][2:]
			fields = row
			continue
		}
		tags := []string{
			"server:" + host,
			"proxy:" + row[0],
			"type:" + row[1],
		}
		var scur, slim float64
		for k, v := range row {
			value, err := strconv.ParseFloat(v, 64)
			if err != nil {
				continue
			}
			field := fields[k]
			if field == "scur" {
				scur = value
			}
			if field == "slim" {
				slim = value
			}

			if name, ok := GAUGES[field]; ok {
				name = fmt.Sprintf("haproxy.%s.%s", strings.ToLower(row[1]), name)
				agg.Add("gauge", metric.NewMetric(name, value, tags))
			}

			if name, ok := RATES[field]; ok {
				name = fmt.Sprintf("haproxy.%s.%s", strings.ToLower(row[1]), name)
				agg.Add("rate", metric.NewMetric(name, value, tags))
			}
		}
		if slim != 0 {
			name := fmt.Sprintf("haproxy.%s.%s", strings.ToLower(row[1]), GAUGES["spct"])
			agg.Add("gauge", metric.NewMetric(name, scur/slim*100, tags))
		}
	}
	return nil
}

func init() {
	collector.Add("haproxy", NewHAProxy)
}
