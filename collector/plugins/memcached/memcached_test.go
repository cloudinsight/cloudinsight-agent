package memcached

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cloudinsight/cloudinsight-agent/common"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
)

const (
	stub = `
STAT pid 1
STAT uptime 23485
STAT time 1483952461
STAT version 1.4.33
STAT libevent 2.0.21-stable
STAT pointer_size 64
STAT rusage_user 2.888000
STAT rusage_system 2.880000
STAT curr_connections 10
STAT total_connections 59
STAT connection_structures 12
STAT reserved_fds 20
STAT cmd_get 14
STAT cmd_set 15
STAT cmd_flush 0
STAT cmd_touch 0
STAT get_hits 13
STAT get_misses 1
STAT get_expired 0
STAT get_flushed 0
STAT delete_misses 0
STAT delete_hits 0
STAT incr_misses 0
STAT incr_hits 0
STAT decr_misses 0
STAT decr_hits 0
STAT cas_misses 1
STAT cas_hits 0
STAT cas_badval 2
STAT touch_hits 0
STAT touch_misses 0
STAT auth_cmds 0
STAT auth_errors 0
STAT bytes_read 1063
STAT bytes_written 63891
STAT limit_maxbytes 67108864
STAT accepting_conns 1
STAT listen_disabled_num 0
STAT time_in_listen_disabled_us 0
STAT threads 4
STAT conn_yields 0
STAT hash_power_level 16
STAT hash_bytes 524288
STAT hash_is_expanding 0
STAT malloc_fails 0
STAT log_worker_dropped 0
STAT log_worker_written 0
STAT log_watcher_skipped 0
STAT log_watcher_sent 0
STAT bytes 403
STAT curr_items 5
STAT total_items 10
STAT expired_unfetched 0
STAT evicted_unfetched 0
STAT evictions 0
STAT reclaimed 0
STAT crawler_reclaimed 0
STAT crawler_items_checked 0
STAT lrutail_reflocked 0
END
`

	stub2 = `
STAT pid 1
STAT uptime 24864
STAT time 1483953840
STAT version 1.4.33
STAT libevent 2.0.21-stable
STAT pointer_size 64
STAT rusage_user 3.076000
STAT rusage_system 3.020000
STAT curr_connections 10
STAT total_connections 77
STAT connection_structures 12
STAT reserved_fds 20
STAT cmd_get 14
STAT cmd_set 15
STAT cmd_flush 0
STAT cmd_touch 0
STAT get_hits 13
STAT get_misses 1
STAT get_expired 0
STAT get_flushed 0
STAT delete_misses 0
STAT delete_hits 0
STAT incr_misses 0
STAT incr_hits 0
STAT decr_misses 0
STAT decr_hits 0
STAT cas_misses 1
STAT cas_hits 0
STAT cas_badval 2
STAT touch_hits 0
STAT touch_misses 0
STAT auth_cmds 0
STAT auth_errors 0
STAT bytes_read 1178
STAT bytes_written 88990
STAT limit_maxbytes 67108864
STAT accepting_conns 1
STAT listen_disabled_num 0
STAT time_in_listen_disabled_us 0
STAT threads 4
STAT conn_yields 0
STAT hash_power_level 16
STAT hash_bytes 524288
STAT hash_is_expanding 0
STAT malloc_fails 0
STAT log_worker_dropped 0
STAT log_worker_written 0
STAT log_watcher_skipped 0
STAT log_watcher_sent 0
STAT bytes 403
STAT curr_items 5
STAT total_items 10
STAT expired_unfetched 0
STAT evicted_unfetched 0
STAT evictions 0
STAT reclaimed 0
STAT crawler_reclaimed 0
STAT crawler_items_checked 0
STAT lrutail_reflocked 0
END
`
)

func TestCollectMetrics(t *testing.T) {
	m := &Memcached{
		Tags: []string{"service:memcached"},
	}
	metricC := make(chan metric.Metric, 100)
	defer close(metricC)
	agg := testutil.MockAggregator(metricC)
	var err error

	stats := bytes.NewBufferString(stub)
	err = m.collectMetrics(stats, m.Tags, agg)
	require.NoError(t, err)
	agg.Flush()
	expectedMetrics := 12
	require.Len(t, metricC, expectedMetrics)

	metrics := make([]metric.Metric, expectedMetrics)
	for i := 0; i < expectedMetrics; i++ {
		metrics[i] = <-metricC
	}

	fields := map[string]float64{
		"memcache.total_items":           10,
		"memcache.curr_items":            5,
		"memcache.limit_maxbytes":        67108864,
		"memcache.uptime":                23485,
		"memcache.bytes":                 403,
		"memcache.curr_connections":      10,
		"memcache.connection_structures": 12,
		"memcache.threads":               4,
		"memcache.pointer_size":          64,
		"memcache.get_hit_percent":       100 * float64(13) / float64(14),
		"memcache.fill_percent":          100 * float64(403) / float64(67108864),
		"memcache.avg_item_size":         100 * float64(403) / float64(5),
	}
	tags := []string{"service:memcached"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	// Wait a second for collecting rate metrics.
	time.Sleep(time.Second)

	stats = bytes.NewBufferString(stub2)
	err = m.collectMetrics(stats, m.Tags, agg)
	require.NoError(t, err)
	agg.Flush()
	expectedMetrics = 28
	require.Len(t, metricC, expectedMetrics)

	metrics = make([]metric.Metric, expectedMetrics)
	for i := 0; i < expectedMetrics; i++ {
		metrics[i] = <-metricC
	}

	fields = map[string]float64{
		"memcache.rusage_user_rate":       0.188,
		"memcache.rusage_system_rate":     0.140,
		"memcache.cmd_get_rate":           0,
		"memcache.cmd_set_rate":           0,
		"memcache.cmd_flush_rate":         0,
		"memcache.get_hits_rate":          0,
		"memcache.get_misses_rate":        0,
		"memcache.delete_misses_rate":     0,
		"memcache.delete_hits_rate":       0,
		"memcache.evictions_rate":         0,
		"memcache.bytes_read_rate":        115,
		"memcache.bytes_written_rate":     25099,
		"memcache.cas_misses_rate":        0,
		"memcache.cas_hits_rate":          0,
		"memcache.cas_badval_rate":        0,
		"memcache.total_connections_rate": 18,
	}
	tags = []string{"service:memcached"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags, 0.0001)
	}
}
