package phpfpm

import (
	"bufio"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/collector"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
)

// NewPHPFPM XXX
func NewPHPFPM(conf plugin.InitConfig) plugin.Plugin {
	return &PHPFPM{}
}

// PHPFPM XXX
type PHPFPM struct {
	StatusURL string `yaml:"status_url"`
	User      string
	Password  string
	Tags      []string
}

var (
	// GAUGES XXX
	GAUGES = map[string]string{
		"listen queue":         "listen_queue.size",
		"max listen queue":     "listen_queue.max_size",
		"idle processes":       "processes.idle",
		"active processes":     "processes.active",
		"total processes":      "processes.total",
		"max active processes": "processes.max_active",
	}

	// MONOTONICCOUNTS XXX
	MONOTONICCOUNTS = map[string]string{
		"accepted conn":        "requests.accepted",
		"max children reached": "processes.max_reached",
		"slow requests":        "requests.slow",
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
func (pf *PHPFPM) Check(agg metric.Aggregator) error {
	addr, err := url.Parse(pf.StatusURL)
	if err != nil {
		return fmt.Errorf("Unable to parse address '%s': %s", pf.StatusURL, err)
	}

	requestURI := addr.String()
	req, err := http.NewRequest("GET", requestURI, nil)
	if err != nil {
		return err
	}
	if pf.User != "" {
		req.SetBasicAuth(pf.User, pf.Password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Unable to connect to phpfpm status page %s: %s", requestURI, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP status %s", requestURI, resp.Status)
	}

	sc := bufio.NewScanner(resp.Body)
	gaugeFields := make(map[string]interface{})
	mcFields := make(map[string]interface{})
	var tags []string
	for sc.Scan() {
		line := sc.Text()
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) < 2 {
				continue
			}
			key, part := parts[0], strings.TrimSpace(parts[1])

			switch key {
			case "pool":
				tags = append(tags, "pool:"+strings.Trim(part, " "))
			default:
				value, err := strconv.ParseFloat(part, 64)
				if err != nil {
					continue
				}

				// Send metric as a gauge, if applicable
				if name, ok := GAUGES[key]; ok {
					gaugeFields[name] = value
				}

				// Send metric as a monotoniccount, if applicable
				if name, ok := MONOTONICCOUNTS[key]; ok {
					mcFields[name] = value
				}
			}
		}
	}
	if tags == nil {
		tags = append(tags, "pool:default")
	}
	tags = append(tags, pf.Tags...)

	if len(gaugeFields) > 0 {
		agg.AddMetrics("gauge", "php_fpm", gaugeFields, tags, "")
	}

	if len(mcFields) > 0 {
		agg.AddMetrics("monotoniccount", "php_fpm", mcFields, tags, "")
	}

	return nil
}

func init() {
	collector.Add("phpfpm", NewPHPFPM)
}
