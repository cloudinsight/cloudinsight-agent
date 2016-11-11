package apache

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

// NewApache XXX
func NewApache(conf plugin.InitConfig) plugin.Plugin {
	return &Apache{}
}

// Apache XXX
type Apache struct {
	ApacheStatusURL string `yaml:"apache_status_url"`
	ApacheUser      string `yaml:"apache_user"`
	ApachePassword  string `yaml:"apache_password"`
	Tags            []string
}

var (
	// GAUGES XXX
	GAUGES = map[string]string{
		"IdleWorkers":    "apache.performance.idle_workers",
		"BusyWorkers":    "apache.performance.busy_workers",
		"CPULoad":        "apache.performance.cpu_load",
		"Uptime":         "apache.performance.uptime",
		"Total kBytes":   "apache.net.bytes",
		"Total Accesses": "apache.net.hits",
	}

	// RATES XXX
	RATES = map[string]string{
		"Total kBytes":   "apache.net.bytes_per_s",
		"Total Accesses": "apache.net.request_per_s",
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
func (a *Apache) Check(agg metric.Aggregator) error {
	addr, err := url.Parse(a.ApacheStatusURL)
	if err != nil {
		return fmt.Errorf("Unable to parse address '%s': %s", a.ApacheStatusURL, err)
	}

	requestURI := addr.String()
	req, err := http.NewRequest("GET", requestURI, nil)
	if err != nil {
		return err
	}
	if a.ApacheUser != "" {
		req.SetBasicAuth(a.ApacheUser, a.ApachePassword)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making HTTP request to %s: %s", requestURI, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP status %s", requestURI, resp.Status)
	}

	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			key, part := parts[0], strings.TrimSpace(parts[1])

			switch key {
			case "Scoreboard":
				continue
			default:
				value, err := strconv.ParseFloat(part, 64)
				if err != nil {
					continue
				}

				// Special case: kBytes => bytes
				if key == "Total kBytes" {
					value = value * 1024
				}

				// Send metric as a gauge, if applicable
				if name, ok := GAUGES[key]; ok {
					agg.Add("gauge", metric.NewMetric(name, value, a.Tags))
				}

				// Send metric as a rate, if applicable
				if name, ok := RATES[key]; ok {
					agg.Add("rate", metric.NewMetric(name, value, a.Tags))
				}
			}
		}
	}

	return nil
}

func init() {
	collector.Add("apache", NewApache)
}
