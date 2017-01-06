package redis

import (
	"testing"

	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudinsight/cloudinsight-agent/common"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
)

var (
	infoResult = `
# Server
redis_version:3.2.6
redis_git_sha1:00000000
redis_git_dirty:0
redis_build_id:3a82d45c1a1c61b0
redis_mode:standalone
os:Linux 4.2.0-36-generic x86_64
arch_bits:64
multiplexing_api:epoll
gcc_version:5.3.0
process_id:1
run_id:ed3eeedd0707db71be123e5736b89bb17d457cec
tcp_port:6379
uptime_in_seconds:38062
uptime_in_days:0
hz:10
lru_clock:7314310
executable:/data/redis-server
config_file:

# Clients
connected_clients:3
client_longest_output_list:0
client_biggest_input_buf:0
blocked_clients:0

# Memory
used_memory:1952056
used_memory_human:1.86M
used_memory_rss:7659520
used_memory_rss_human:7.30M
used_memory_peak:1993112
used_memory_peak_human:1.90M
total_system_memory:3820826624
total_system_memory_human:3.56G
used_memory_lua:37888
used_memory_lua_human:37.00K
maxmemory:0
maxmemory_human:0B
maxmemory_policy:noeviction
mem_fragmentation_ratio:3.92
mem_allocator:jemalloc-4.0.3

# Persistence
loading:0
rdb_changes_since_last_save:0
rdb_bgsave_in_progress:0
rdb_last_save_time:1483700140
rdb_last_bgsave_status:ok
rdb_last_bgsave_time_sec:0
rdb_current_bgsave_time_sec:-1
aof_enabled:0
aof_rewrite_in_progress:0
aof_rewrite_scheduled:0
aof_last_rewrite_time_sec:-1
aof_current_rewrite_time_sec:-1
aof_last_bgrewrite_status:ok
aof_last_write_status:ok

# Stats
total_connections_received:74
total_commands_processed:41181
instantaneous_ops_per_sec:1
total_net_input_bytes:1537930
total_net_output_bytes:17099987
instantaneous_input_kbps:0.05
instantaneous_output_kbps:0.00
rejected_connections:0
sync_full:1
sync_partial_ok:0
sync_partial_err:0
expired_keys:0
evicted_keys:0
keyspace_hits:0
keyspace_misses:0
pubsub_channels:0
pubsub_patterns:0
latest_fork_usec:903
migrate_cached_sockets:0

# Replication
role:master
connected_slaves:1
slave0:ip=172.17.0.3,port=6379,state=online,offset=53445,lag=1
master_repl_offset:53445
repl_backlog_active:1
repl_backlog_size:1048576
repl_backlog_first_byte_offset:2
repl_backlog_histlen:53444

# CPU
used_cpu_sys:26.26
used_cpu_user:11.17
used_cpu_sys_children:26.26
used_cpu_user_children:11.17

# Commandstats
cmdstat_set:calls=7,usec=37,usec_per_call=5.29
cmdstat_ping:calls=1,usec=1,usec_per_call=1.00
cmdstat_psync:calls=1,usec=1975,usec_per_call=1975.00
cmdstat_replconf:calls=37977,usec=45596,usec_per_call=1.20
cmdstat_info:calls=1072,usec=85334,usec_per_call=79.60
cmdstat_config:calls=1056,usec=19788,usec_per_call=18.74
cmdstat_slowlog:calls=1065,usec=70611,usec_per_call=66.30
cmdstat_command:calls=2,usec=1751,usec_per_call=875.50

# Cluster
cluster_enabled:0

# Keyspace
db0:keys=3,expires=0,avg_ttl=0
`

	slowlogResult = []interface{}{
		[]interface{}{
			int64(950),
			int64(1483706756),
			int64(144),
			[]interface{}{
				[]uint8("SLOWLOG"),
				[]uint8("GET"),
				[]uint8("128"),
			},
		},
		[]interface{}{
			int64(949),
			int64(1483706756),
			int64(13),
			[]interface{}{
				[]uint8("CONFIG"),
				[]uint8("GET"),
				[]uint8("slowlog-max-len"),
			},
		},
		[]interface{}{
			int64(948),
			int64(1483706756),
			int64(60),
			[]interface{}{
				[]uint8("INFO"),
			},
		},
		[]interface{}{
			int64(947),
			int64(1483706717),
			int64(121),
			[]interface{}{
				[]uint8("SLOWLOG"),
				[]uint8("GET"),
				[]uint8("128"),
			},
		},
		[]interface{}{
			int64(946),
			int64(1483706717),
			int64(15),
			[]interface{}{
				[]uint8("CONFIG"),
				[]uint8("GET"),
				[]uint8("slowlog-max-len"),
			},
		},
	}

	slowlogMaxLenResult = []interface{}{[]byte("slowlog-max-len"), []byte("5")}
)

func TestCollectMetrics(t *testing.T) {
	r := Redis{
		Host:              "localhost",
		Port:              6379,
		Tags:              []string{"service:redis"},
		Keys:              []string{"hash", "list", "set", "sset", "string"},
		WarnOnMissingKeys: true,
		lastTimestampSeen: make(map[instance]int64),
	}
	metricC := make(chan metric.Metric, 100)
	defer close(metricC)
	agg := testutil.MockAggregator(metricC)
	c := redigomock.NewConn()
	var err error

	c.Command("INFO", "ALL").Expect(infoResult)
	c.Command("CONFIG", "GET", "slowlog-max-len").Expect(slowlogMaxLenResult)
	c.Command("SLOWLOG", "GET", float64(5)).Expect(slowlogResult)
	c.Command("HLEN", "hash").Expect(3)
	c.Command("LLEN", "list").Expect(4)
	c.Command("SCARD", "set").Expect(5)
	c.Command("ZCARD", "sset").Expect(6)

	err = r.collectMetrics(c, r.Tags, agg)
	require.NoError(t, err)
	agg.Flush()
	expectedMetrics := 48
	require.Len(t, metricC, expectedMetrics)

	metrics := make([]metric.Metric, expectedMetrics)
	for i := 0; i < expectedMetrics; i++ {
		metrics[i] = <-metricC
	}

	fields := map[string]float64{
		// Append-only metrics
		"redis.aof.last_rewrite_time": -1,
		"redis.aof.rewrite":           0,
		// "redis.aof.size":              0,
		// "redis.aof.buffer_length":     0,

		// Network
		"redis.net.clients":  3,
		"redis.net.slaves":   1,
		"redis.net.rejected": 0,

		// clients
		"redis.clients.blocked":             0,
		"redis.clients.biggest_input_buf":   0,
		"redis.clients.longest_output_list": 0,

		// Keys
		"redis.keys.evicted": 0,
		"redis.keys.expired": 0,

		// stats
		"redis.perf.latest_fork_usec": 903,

		// pubsub
		"redis.pubsub.channels": 0,
		"redis.pubsub.patterns": 0,

		// rdb
		"redis.rdb.bgsave":             0,
		"redis.rdb.changes_since_last": 0,
		"redis.rdb.last_bgsave_time":   0,

		// memory
		"redis.mem.fragmentation_ratio": 3.92,
		"redis.mem.used":                1952056,
		"redis.mem.lua":                 37888,
		"redis.mem.peak":                1993112,
		"redis.mem.rss":                 7659520,

		// replication
		// "redis.replication.last_io_seconds_ago": 0,
		// "redis.replication.sync":               0,
		// "redis.replication.sync_left_bytes":    0,
		"redis.replication.backlog_histlen":    53444,
		"redis.replication.master_repl_offset": 53445,
		// "redis.replication.slave_repl_offset":  0,
		// "redis.replication.master_link_down_since_seconds": 0,
	}
	tags := []string{"service:redis"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	// db
	fields = map[string]float64{
		"redis.persist":         3,
		"redis.persist.percent": 100,
		"redis.expires.percent": 0,
	}
	tags = []string{"service:redis", "redis_db:db0"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	// replication
	tags = []string{"service:redis", "slave_ip:172.17.0.3", "slave_port:6379", "slave_id:0"}
	testutil.AssertContainsMetricWithTags(t, metrics, "redis.replication.delay", 0, tags)

	// keys
	testutil.AssertContainsMetricWithTags(t, metrics, "redis.key.length", 3, []string{"service:redis", "key:hash"})
	testutil.AssertContainsMetricWithTags(t, metrics, "redis.key.length", 4, []string{"service:redis", "key:list"})
	testutil.AssertContainsMetricWithTags(t, metrics, "redis.key.length", 5, []string{"service:redis", "key:set"})
	testutil.AssertContainsMetricWithTags(t, metrics, "redis.key.length", 6, []string{"service:redis", "key:sset"})
	testutil.AssertContainsMetricWithTags(t, metrics, "redis.key.length", 0, []string{"service:redis", "key:string"})

	// slowlog
	fields = map[string]float64{
		"redis.slowlog.micros.95percentile": 60,
		"redis.slowlog.micros.avg":          60,
		"redis.slowlog.micros.count":        1,
		"redis.slowlog.micros.max":          60,
		"redis.slowlog.micros.median":       60,
	}
	tags = []string{"service:redis", "command:INFO"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	fields = map[string]float64{
		"redis.slowlog.micros.95percentile": 15,
		"redis.slowlog.micros.avg":          14,
		"redis.slowlog.micros.count":        2,
		"redis.slowlog.micros.max":          15,
		"redis.slowlog.micros.median":       13,
	}
	tags = []string{"service:redis", "command:CONFIG"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	fields = map[string]float64{
		"redis.slowlog.micros.95percentile": 144,
		"redis.slowlog.micros.avg":          132.5,
		"redis.slowlog.micros.count":        2,
		"redis.slowlog.micros.max":          144,
		"redis.slowlog.micros.median":       121,
	}
	tags = []string{"service:redis", "command:SLOWLOG"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}
}

func TestGetOptions(t *testing.T) {
	r := Redis{
		DB:            1,
		Password:      "test",
		SocketTimeout: 2,
	}
	assert.Len(t, r.getOptions(), 3)
}

func TestGetTags(t *testing.T) {
	r := Redis{
		Host: "localhost",
		Port: 6379,
		Tags: []string{"service:redis"},
	}
	assert.Equal(t, []string{"service:redis", "redis_host:localhost", "redis_port:6379"}, r.getTags())
}

func TestGenerateInstance(t *testing.T) {
	r := Redis{
		Host: "localhost",
		Port: 6379,
		DB:   1,
	}
	assert.Equal(t, instance{"localhost:6379", "1"}, r.generateInstance())

	r.UnixSocketPath = "/tmp/redis.sock"
	assert.Equal(t, instance{"/tmp/redis.sock", "1"}, r.generateInstance())
}
