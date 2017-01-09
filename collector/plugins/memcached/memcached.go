package memcached

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/cloudinsight/cloudinsight-agent/collector"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
)

// NewMemcached XXX
func NewMemcached(conf plugin.InitConfig) plugin.Plugin {
	return &Memcached{}
}

// Memcached XXX
type Memcached struct {
	URL    string
	Port   int
	Socket string
	Tags   []string
}

var (
	// GAUGES XXX
	GAUGES = map[string]string{
		"total_items":           "memcache.total_items",
		"curr_items":            "memcache.curr_items",
		"limit_maxbytes":        "memcache.limit_maxbytes",
		"uptime":                "memcache.uptime",
		"bytes":                 "memcache.bytes",
		"curr_connections":      "memcache.curr_connections",
		"connection_structures": "memcache.connection_structures",
		"threads":               "memcache.threads",
		"pointer_size":          "memcache.pointer_size",
	}

	// RATES XXX
	RATES = map[string]string{
		"rusage_user":       "memcache.rusage_user_rate",
		"rusage_system":     "memcache.rusage_system_rate",
		"cmd_get":           "memcache.cmd_get_rate",
		"cmd_set":           "memcache.cmd_set_rate",
		"cmd_flush":         "memcache.cmd_flush_rate",
		"get_hits":          "memcache.get_hits_rate",
		"get_misses":        "memcache.get_misses_rate",
		"delete_misses":     "memcache.delete_misses_rate",
		"delete_hits":       "memcache.delete_hits_rate",
		"evictions":         "memcache.evictions_rate",
		"bytes_read":        "memcache.bytes_read_rate",
		"bytes_written":     "memcache.bytes_written_rate",
		"cas_misses":        "memcache.cas_misses_rate",
		"cas_hits":          "memcache.cas_hits_rate",
		"cas_badval":        "memcache.cas_badval_rate",
		"total_connections": "memcache.total_connections_rate",
	}
)

// Check XXX
func (m *Memcached) Check(agg metric.Aggregator) error {
	network := "tcp"
	target := fmt.Sprintf("%s:%d", m.URL, m.Port)
	if m.Socket != "" {
		network = "unix"
		target = m.Socket
	}
	conn, err := net.Dial(network, target)
	if err != nil {
		return err
	}
	fmt.Fprintln(conn, "stats")
	tags := append(m.Tags, fmt.Sprintf("url:%s:%d", m.URL, m.Port))
	err = m.collectMetrics(conn, tags, agg)
	if err != nil {
		return err
	}
	return nil
}

func (m *Memcached) collectMetrics(conn io.Reader, tags []string, agg metric.Aggregator) error {
	stats, err := m.parseStats(conn)
	if err != nil {
		return err
	}

	for key, value := range stats {
		if name, ok := GAUGES[key]; ok {
			agg.Add("gauge", metric.Metric{
				Name:  name,
				Value: value,
				Tags:  tags,
			})
		}

		if name, ok := RATES[key]; ok {
			agg.Add("rate", metric.Metric{
				Name:  name,
				Value: value,
				Tags:  tags,
			})
		}
	}

	if cmdGet, ok := stats["cmd_get"]; ok && cmdGet > 0 {
		if getHits, ok := stats["get_hits"]; ok {
			agg.Add("gauge", metric.Metric{
				Name:  "memcache.get_hit_percent",
				Value: 100 * getHits / cmdGet,
				Tags:  tags,
			})
		}
	}
	if limitMaxBytes, ok := stats["limit_maxbytes"]; ok && limitMaxBytes > 0 {
		if bytes, ok := stats["bytes"]; ok {
			agg.Add("gauge", metric.Metric{
				Name:  "memcache.fill_percent",
				Value: 100 * bytes / limitMaxBytes,
				Tags:  tags,
			})
		}
	}
	if currItems, ok := stats["curr_items"]; ok && currItems > 0 {
		if bytes, ok := stats["bytes"]; ok {
			agg.Add("gauge", metric.Metric{
				Name:  "memcache.avg_item_size",
				Value: 100 * bytes / currItems,
				Tags:  tags,
			})
		}
	}
	return nil
}

func (m *Memcached) parseStats(conn io.Reader) (map[string]float64, error) {
	scanner := bufio.NewScanner(conn)
	stats := make(map[string]float64)

	for scanner.Scan() {
		line := scanner.Text()
		s := string(line)
		if s == "END" {
			return stats, nil
		}

		res := strings.Split(s, " ")
		if res[0] == "STAT" {
			key := res[1]
			val, err := strconv.ParseFloat(res[2], 64)
			if err != nil {
				continue
			}
			stats[key] = val
		}
	}
	if err := scanner.Err(); err != nil {
		return stats, err
	}
	return nil, nil
}

func init() {
	collector.Add("mcache", NewMemcached)
}
