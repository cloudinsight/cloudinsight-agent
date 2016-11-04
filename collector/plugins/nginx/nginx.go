package nginx

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

// NewNginx XXX
func NewNginx(conf plugin.InitConfig) plugin.Plugin {
	return &Nginx{}
}

// Nginx XXX
type Nginx struct {
	NginxStatusURL string `yaml:"nginx_status_url"`
	Tags           []string
}

var tr = &http.Transport{
	ResponseHeaderTimeout: time.Duration(3 * time.Second),
}

var client = &http.Client{
	Transport: tr,
	Timeout:   time.Duration(4 * time.Second),
}

// Check XXX
func (n *Nginx) Check(agg metric.Aggregator) error {
	addr, err := url.Parse(n.NginxStatusURL)
	if err != nil {
		return fmt.Errorf("Unable to parse address '%s': %s", n.NginxStatusURL, err)
	}

	resp, err := client.Get(addr.String())
	if err != nil {
		return fmt.Errorf("error making HTTP request to %s: %s", addr.String(), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP status %s", addr.String(), resp.Status)
	}
	r := bufio.NewReader(resp.Body)

	// Active connections
	_, err = r.ReadString(':')
	if err != nil {
		return err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	active, err := strconv.ParseUint(strings.TrimSpace(line), 10, 64)
	if err != nil {
		return err
	}

	// Server accepts handled requests
	_, err = r.ReadString('\n')
	if err != nil {
		return err
	}
	line, err = r.ReadString('\n')
	if err != nil {
		return err
	}
	data := strings.Fields(line)
	accepts, err := strconv.ParseUint(data[0], 10, 64)
	if err != nil {
		return err
	}

	handled, err := strconv.ParseUint(data[1], 10, 64)
	if err != nil {
		return err
	}
	requests, err := strconv.ParseUint(data[2], 10, 64)
	if err != nil {
		return err
	}

	// Reading/Writing/Waiting
	line, err = r.ReadString('\n')
	if err != nil {
		return err
	}
	data = strings.Fields(line)
	reading, err := strconv.ParseUint(data[1], 10, 64)
	if err != nil {
		return err
	}
	writing, err := strconv.ParseUint(data[3], 10, 64)
	if err != nil {
		return err
	}
	waiting, err := strconv.ParseUint(data[5], 10, 64)
	if err != nil {
		return err
	}

	fields := map[string]interface{}{
		"connections": active,
		"reading":     reading,
		"writing":     writing,
		"waiting":     waiting,
	}
	agg.AddMetrics("gauge", "nginx.net", fields, n.Tags, "")

	fields = map[string]interface{}{
		"conn_opened_per_s":  accepts,
		"conn_dropped_per_s": accepts - handled,
		"request_per_s":      requests,
	}
	agg.AddMetrics("rate", "nginx.net", fields, n.Tags, "")

	return nil
}

func init() {
	collector.Add("nginx", NewNginx)
}
