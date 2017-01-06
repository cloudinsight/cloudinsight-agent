package redis

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/cloudinsight/cloudinsight-agent/collector"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
	"github.com/cloudinsight/cloudinsight-agent/common/util"
)

// NewRedis XXX
func NewRedis(conf plugin.InitConfig) plugin.Plugin {
	return &Redis{
		lastTimestampSeen: make(map[instance]int64),
	}
}

// Redis XXX
type Redis struct {
	Host              string
	Port              int
	UnixSocketPath    string `yaml:"unix_socket_path"`
	DB                int
	Password          string
	SocketTimeout     int `yaml:"socket_timeout"`
	Tags              []string
	Keys              []string
	WarnOnMissingKeys bool    `yaml:"warn_on_missing_keys"`
	SlowlogMaxLen     float64 `yaml:"slowlog-max-len"`

	lastTimestampSeen map[instance]int64
}

type instance [2]string

var (
	// GAUGES XXX
	GAUGES = map[string]string{
		// Append-only metrics
		"aof_last_rewrite_time_sec": "redis.aof.last_rewrite_time",
		"aof_rewrite_in_progress":   "redis.aof.rewrite",
		"aof_current_size":          "redis.aof.size",
		"aof_buffer_length":         "redis.aof.buffer_length",

		// Network
		"connected_clients":    "redis.net.clients",
		"connected_slaves":     "redis.net.slaves",
		"rejected_connections": "redis.net.rejected",

		// clients
		"blocked_clients":            "redis.clients.blocked",
		"client_biggest_input_buf":   "redis.clients.biggest_input_buf",
		"client_longest_output_list": "redis.clients.longest_output_list",

		// Keys
		"evicted_keys": "redis.keys.evicted",
		"expired_keys": "redis.keys.expired",

		// stats
		"latest_fork_usec": "redis.perf.latest_fork_usec",

		// pubsub
		"pubsub_channels": "redis.pubsub.channels",
		"pubsub_patterns": "redis.pubsub.patterns",

		// rdb
		"rdb_bgsave_in_progress":      "redis.rdb.bgsave",
		"rdb_changes_since_last_save": "redis.rdb.changes_since_last",
		"rdb_last_bgsave_time_sec":    "redis.rdb.last_bgsave_time",

		// memory
		"mem_fragmentation_ratio": "redis.mem.fragmentation_ratio",
		"used_memory":             "redis.mem.used",
		"used_memory_lua":         "redis.mem.lua",
		"used_memory_peak":        "redis.mem.peak",
		"used_memory_rss":         "redis.mem.rss",

		// replication
		"master_last_io_seconds_ago": "redis.replication.last_io_seconds_ago",
		"master_sync_in_progress":    "redis.replication.sync",
		"master_sync_left_bytes":     "redis.replication.sync_left_bytes",
		"repl_backlog_histlen":       "redis.replication.backlog_histlen",
		"master_repl_offset":         "redis.replication.master_repl_offset",
		"slave_repl_offset":          "redis.replication.slave_repl_offset",
	}

	// RATES XXX
	RATES = map[string]string{
		// cpu
		"used_cpu_sys":           "redis.cpu.sys",
		"used_cpu_sys_children":  "redis.cpu.sys_children",
		"used_cpu_user":          "redis.cpu.user",
		"used_cpu_user_children": "redis.cpu.user_children",

		// stats
		"total_commands_processed": "redis.net.commands",
		"keyspace_hits":            "redis.stats.keyspace_hits",
		"keyspace_misses":          "redis.stats.keyspace_misses",
	}
)

// Check XXX
func (r *Redis) Check(agg metric.Aggregator) error {
	network := "tcp"
	target := fmt.Sprintf("%s:%d", r.Host, r.Port)
	if r.UnixSocketPath != "" {
		target = r.UnixSocketPath
		network = "unix"
	}

	options := r.getOptions()
	c, err := redis.Dial(network, target, options...)
	if err != nil {
		log.Errorf("Failed to connect redis. %s", err.Error())
		return err
	}
	defer c.Close()

	tags := r.getTags()
	err = r.collectMetrics(c, tags, agg)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) collectMetrics(c redis.Conn, tags []string, agg metric.Aggregator) error {
	err := r.collectInfoMetrics(c, tags, agg)
	if err != nil {
		return err
	}

	r.collectKeysLength(c, tags, agg)

	err = r.collectSlowlog(c, tags, agg)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) collectInfoMetrics(c redis.Conn, tags []string, agg metric.Aggregator) error {
	start := time.Now()
	info, err := redis.String(c.Do("INFO", "ALL"))
	if err != nil {
		log.Errorf("Failed to run info command. %s", err.Error())
		return err
	}
	elapsed := time.Since(start)
	latencyMs := util.Round(float64(elapsed)/float64(time.Millisecond), 2)
	agg.Add("gauge", metric.Metric{
		Name:  "redis.info.latency_ms",
		Value: latencyMs,
		Tags:  tags,
	})

	lines := strings.Split(info, "\r\n")
	// Compatible with the raw string literals for test reason.
	if len(lines) == 1 {
		lines = strings.Split(info, "\n")
	}

	for _, line := range lines {
		if line == "" {
			continue
		}
		if re, _ := regexp.MatchString("^#", line); re {
			continue
		}

		record := strings.SplitN(line, ":", 2)
		if len(record) < 2 {
			continue
		}
		key, value := record[0], record[1]

		if re, _ := regexp.MatchString(`^db\d+`, key); re {
			r.collectDBMetrics(key, value, tags, agg)
			continue
		}

		val, err := strconv.ParseFloat(value, 64)
		if err != nil {
			continue
		}

		if name, ok := GAUGES[key]; ok {
			agg.Add("gauge", metric.Metric{
				Name:  name,
				Value: val,
				Tags:  tags,
			})
		}

		if name, ok := RATES[key]; ok {
			agg.Add("rate", metric.Metric{
				Name:  name,
				Value: val,
				Tags:  tags,
			})
		}
	}

	r.collectReplicaMetrics(lines, tags, agg)
	return nil
}

func (r *Redis) collectDBMetrics(key, value string, tags []string, agg metric.Aggregator) {
	kv := strings.SplitN(value, ",", 3)
	if len(kv) != 3 {
		return
	}
	keys, expired := kv[0], kv[1]

	totalKeys, err := extractVal(keys)
	if err != nil {
		log.Warnf("Failed to parse db keys. %s", err)
	}

	expiredKeys, err := extractVal(expired)
	if err != nil {
		log.Warnf("Failed to parse db expired. %s", err)
	}

	dbTags := append(tags, "redis_db:"+key)
	persistKeys := totalKeys - expiredKeys
	fields := map[string]interface{}{
		"persist":         persistKeys,
		"persist.percent": 100 * persistKeys / totalKeys,
		"expires.percent": 100 * expiredKeys / totalKeys,
	}
	agg.AddMetrics("gauge", "redis", fields, dbTags, "")
}

func (r *Redis) collectReplicaMetrics(lines, tags []string, agg metric.Aggregator) {
	var masterDownSeconds, masterOffset, slaveOffset float64
	var masterStatus, slaveID, ip, port string
	var err error
	for _, line := range lines {
		record := strings.SplitN(line, ":", 2)
		if len(record) < 2 {
			continue
		}
		key, value := record[0], record[1]

		if key == "master_repl_offset" {
			masterOffset, _ = strconv.ParseFloat(value, 64)
		}

		if key == "master_link_down_since_seconds" {
			masterDownSeconds, _ = strconv.ParseFloat(value, 64)
		}

		if key == "master_link_status" {
			masterStatus = value
		}

		if re, _ := regexp.MatchString(`^slave\d+`, key); re {
			slaveID = strings.TrimPrefix(key, "slave")
			kv := strings.SplitN(value, ",", 5)
			if len(kv) != 5 {
				continue
			}

			split := strings.Split(kv[0], "=")
			if len(split) != 2 {
				log.Warnf("Failed to parse slave ip. %s", err)
				continue
			}
			ip = split[1]

			split = strings.Split(kv[1], "=")
			if err != nil {
				log.Warnf("Failed to parse slave port. %s", err)
				continue
			}
			port = split[1]

			split = strings.Split(kv[3], "=")
			if err != nil {
				log.Warnf("Failed to parse slave offset. %s", err)
				continue
			}
			slaveOffset, _ = strconv.ParseFloat(split[1], 64)
		}
	}

	delay := masterOffset - slaveOffset
	slaveTags := append(tags, "slave_ip:"+ip, "slave_port:"+port, "slave_id:"+slaveID)
	if delay >= 0 {
		agg.Add("gauge", metric.Metric{
			Name:  "redis.replication.delay",
			Value: delay,
			Tags:  slaveTags,
		})
	}

	if masterStatus != "" {
		agg.Add("gauge", metric.Metric{
			Name:  "redis.replication.master_link_down_since_seconds",
			Value: masterDownSeconds,
			Tags:  tags,
		})
	}
}

func (r *Redis) collectKeysLength(c redis.Conn, tags []string, agg metric.Aggregator) {
	for _, key := range r.Keys {
		found := false
		keyTags := append(tags, "key:"+key)

		for _, op := range []string{
			"HLEN",
			"LLEN",
			"SCARD",
			"ZCARD",
			"PFCOUNT",
			"STRLEN",
		} {
			if val, err := c.Do(op, key); err == nil && val != nil {
				found = true
				agg.Add("gauge", metric.Metric{
					Name:  "redis.key.length",
					Value: val,
					Tags:  keyTags,
				})
				break
			}
		}

		if !found {
			if r.WarnOnMissingKeys {
				log.Warnf("%s key not found in redis", key)
			}
			agg.Add("gauge", metric.Metric{
				Name:  "redis.key.length",
				Value: 0,
				Tags:  keyTags,
			})
		}
	}
}

func (r *Redis) collectSlowlog(c redis.Conn, tags []string, agg metric.Aggregator) error {
	var maxSlowEntries, defaultMaxSlowEntries float64
	defaultMaxSlowEntries = 128
	if r.SlowlogMaxLen > 0 {
		maxSlowEntries = r.SlowlogMaxLen
	} else {
		if config, err := redis.Strings(c.Do("CONFIG", "GET", "slowlog-max-len")); err == nil {
			fields, err := extractConfig(config)
			if err != nil {
				return nil
			}

			maxSlowEntries = fields["slowlog-max-len"].(float64)
			if maxSlowEntries > defaultMaxSlowEntries {
				maxSlowEntries = defaultMaxSlowEntries
			}
		} else {
			maxSlowEntries = defaultMaxSlowEntries
		}
	}

	// Generate a unique id for this instance to be persisted across runs
	tsKey := r.generateInstance()

	slowlogs, err := redis.Values(c.Do("SLOWLOG", "GET", maxSlowEntries))
	if err != nil {
		return err
	}

	var maxTs int64
	for _, slowlog := range slowlogs {
		if entry, ok := slowlog.([]interface{}); ok {
			if entry == nil || len(entry) != 4 {
				return errors.New("slowlog get protocol error")
			}

			// id := entry[0].(int64)
			startTime := entry[1].(int64)
			if startTime <= r.lastTimestampSeen[tsKey] {
				continue
			}
			if startTime > maxTs {
				maxTs = startTime
			}
			duration := entry[2].(int64)

			var command []string
			if obj, ok := entry[3].([]interface{}); ok {
				for _, arg := range obj {
					command = append(command, string(arg.([]uint8)))
				}
			}

			commandTags := append(tags, "command:"+command[0])
			agg.Add("histogram", metric.Metric{
				Name:  "redis.slowlog.micros",
				Value: duration,
				Tags:  commandTags,
			})
		}
	}
	r.lastTimestampSeen[tsKey] = maxTs
	return nil
}

func (r *Redis) getOptions() []redis.DialOption {
	var options []redis.DialOption
	if r.DB > 0 {
		options = append(options, redis.DialDatabase(r.DB))
	}
	if r.Password != "" {
		options = append(options, redis.DialPassword(r.Password))
	}
	if r.SocketTimeout > 0 {
		options = append(options, redis.DialConnectTimeout(time.Duration(r.SocketTimeout)*time.Second))
	}
	return options
}

func (r *Redis) getTags() []string {
	var tags []string
	if r.UnixSocketPath != "" {
		tags = append(r.Tags, "redis_host:"+r.UnixSocketPath, "redis_port:unix_socket")
	} else {
		tags = append(r.Tags, "redis_host:"+r.Host, "redis_port:"+strconv.Itoa(r.Port))
	}
	return tags
}

func (r *Redis) generateInstance() instance {
	if r.UnixSocketPath != "" {
		return instance{r.UnixSocketPath, strconv.Itoa(r.DB)}
	}
	return instance{fmt.Sprintf("%s:%d", r.Host, r.Port), strconv.Itoa(r.DB)}
}

func extractVal(s string) (val float64, err error) {
	split := strings.Split(s, "=")
	if len(split) != 2 {
		return 0, fmt.Errorf("nope")
	}
	val, err = strconv.ParseFloat(split[1], 64)
	if err != nil {
		return 0, fmt.Errorf("nope")
	}
	return
}

func extractConfig(config []string) (map[string]interface{}, error) {
	fields := make(map[string]interface{})

	if len(config)%2 != 0 {
		return nil, fmt.Errorf("invalid config: %#v", config)
	}

	for pos := 0; pos < len(config)/2; pos++ {
		val, err := strconv.ParseFloat(config[pos*2+1], 64)
		if err != nil {
			log.Debugf("couldn't parse %s, err: %s", config[pos*2+1], err)
			continue
		}
		fields[config[pos*2]] = val
	}
	return fields, nil
}

func init() {
	collector.Add("redisdb", NewRedis)
}
