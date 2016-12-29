package mongodb

import (
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/collector"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// NewMongoDB XXX
func NewMongoDB(conf plugin.InitConfig) plugin.Plugin {
	return &MongoDB{}
}

// MongoDB XXX
type MongoDB struct {
	Server            string
	Timeout           int64
	Tags              []string
	AdditionalMetrics []string `yaml:"additional_metrics"`
}

// ReplSetStatus stores information from replSetGetStatus
type ReplSetStatus struct {
	Members []ReplSetMember `bson:"members"`
	MyState int64           `bson:"myState"`
}

// ReplSetMember stores information related to a replica set member
type ReplSetMember struct {
	Name       string    `bson:"name"`
	Health     int64     `bson:"health"`
	State      int64     `bson:"state"`
	StateStr   string    `bson:"stateStr"`
	OptimeDate time.Time `bson:"optimeDate"`
	Self       bool      `bson:"self"`
}

// Top stores information from replSetGetStatus
type Top struct {
	Totals map[string]bson.M `bson:"totals"`
}

// Session is an interface to access to the Session struct.
type Session interface {
	Run(cmd interface{}, result interface{}) error
	DB(name string) DataLayer
	DatabaseNames() (names []string, err error)
	Close()
}

// MongoSession is currently a Mongo session.
type MongoSession struct {
	*mgo.Session
}

// DB shadows *mgo.DB to returns a DataLayer interface instead of *mgo.Database.
func (s MongoSession) DB(name string) DataLayer {
	return s.Session.DB(name)
}

// DataLayer is an interface to access to the database struct.
type DataLayer interface {
	Run(cmd interface{}, result interface{}) error
}

const (
	// metric types
	gauge = "gauge"
	rate  = "rate"
)

var (
	// MongoDB replica set states, as documented at
	// https://docs.mongodb.org/manual/reference/replica-states/
	replsetStates = map[int64]string{
		0:  "startup",
		1:  "primary",
		2:  "secondary",
		3:  "recovering",
		5:  "startup2",
		6:  "unknown",
		7:  "arbiter",
		8:  "down",
		9:  "rollback",
		10: "removed",
	}

	// METRIC LIST DEFINITION

	// Format
	// ------
	//   metric_name -> (metric_type, alias)
	// or
	//   metric_name -> metric_type *
	// * by default MongoDB metrics are reported under their original metric names

	// serverStatus metrics collected by default.
	// https://docs.mongodb.com/manual/reference/command/serverStatus/
	serverStatusMetrics = map[string]string{
		"asserts.msg":                                          rate,
		"asserts.regular":                                      rate,
		"asserts.rollovers":                                    rate,
		"asserts.user":                                         rate,
		"asserts.warning":                                      rate,
		"backgroundFlushing.average_ms":                        gauge,
		"backgroundFlushing.flushes":                           rate,
		"backgroundFlushing.last_ms":                           gauge,
		"backgroundFlushing.total_ms":                          gauge,
		"connections.available":                                gauge,
		"connections.current":                                  gauge,
		"connections.totalCreated":                             gauge,
		"cursors.timedOut":                                     gauge,
		"cursors.totalOpen":                                    gauge,
		"extra_info.heap_usage_bytes":                          rate,
		"extra_info.page_faults":                               rate,
		"globalLock.activeClients.readers":                     gauge,
		"globalLock.activeClients.total":                       gauge,
		"globalLock.activeClients.writers":                     gauge,
		"globalLock.currentQueue.readers":                      gauge,
		"globalLock.currentQueue.total":                        gauge,
		"globalLock.currentQueue.writers":                      gauge,
		"globalLock.lockTime":                                  gauge,
		"globalLock.ratio":                                     gauge, // < 2.2
		"globalLock.totalTime":                                 gauge,
		"indexCounters.accesses":                               rate,
		"indexCounters.btree.accesses":                         rate,  // < 2.4
		"indexCounters.btree.hits":                             rate,  // < 2.4
		"indexCounters.btree.misses":                           rate,  // < 2.4
		"indexCounters.btree.missRatio":                        gauge, // < 2.4
		"indexCounters.hits":                                   rate,
		"indexCounters.misses":                                 rate,
		"indexCounters.missRatio":                              gauge,
		"indexCounters.resets":                                 rate,
		"mem.bits":                                             gauge,
		"mem.mapped":                                           gauge,
		"mem.mappedWithJournal":                                gauge,
		"mem.resident":                                         gauge,
		"mem.virtual":                                          gauge,
		"metrics.cursor.open.noTimeout":                        gauge,
		"metrics.cursor.open.pinned":                           gauge,
		"metrics.cursor.open.total":                            gauge,
		"metrics.cursor.timedOut":                              rate,
		"metrics.document.deleted":                             rate,
		"metrics.document.inserted":                            rate,
		"metrics.document.returned":                            rate,
		"metrics.document.updated":                             rate,
		"metrics.getLastError.wtime.num":                       rate,
		"metrics.getLastError.wtime.totalMillis":               rate,
		"metrics.getLastError.wtimeouts":                       rate,
		"metrics.operation.fastmod":                            rate,
		"metrics.operation.idhack":                             rate,
		"metrics.operation.scanAndOrder":                       rate,
		"metrics.operation.writeConflicts":                     rate,
		"metrics.queryExecutor.scanned":                        rate,
		"metrics.record.moves":                                 rate,
		"metrics.repl.apply.batches.num":                       rate,
		"metrics.repl.apply.batches.totalMillis":               rate,
		"metrics.repl.apply.ops":                               rate,
		"metrics.repl.buffer.count":                            gauge,
		"metrics.repl.buffer.maxSizeBytes":                     gauge,
		"metrics.repl.buffer.sizeBytes":                        gauge,
		"metrics.repl.network.bytes":                           rate,
		"metrics.repl.network.getmores.num":                    rate,
		"metrics.repl.network.getmores.totalMillis":            rate,
		"metrics.repl.network.ops":                             rate,
		"metrics.repl.network.readersCreated":                  rate,
		"metrics.repl.oplog.insert.num":                        rate,
		"metrics.repl.oplog.insert.totalMillis":                rate,
		"metrics.repl.oplog.insertBytes":                       rate,
		"metrics.repl.preload.docs.num":                        rate,
		"metrics.repl.preload.docs.totalMillis":                rate,
		"metrics.repl.preload.indexes.num":                     rate,
		"metrics.repl.preload.indexes.totalMillis":             rate,
		"metrics.repl.storage.freelist.search.bucketExhausted": rate,
		"metrics.repl.storage.freelist.search.requests":        rate,
		"metrics.repl.storage.freelist.search.scanned":         rate,
		"metrics.ttl.deletedDocuments":                         rate,
		"metrics.ttl.passes":                                   rate,
		"network.bytesIn":                                      rate,
		"network.bytesOut":                                     rate,
		"network.numRequests":                                  rate,
		"opcounters.command":                                   rate,
		"opcounters.delete":                                    rate,
		"opcounters.getmore":                                   rate,
		"opcounters.insert":                                    rate,
		"opcounters.query":                                     rate,
		"opcounters.update":                                    rate,
		"opcountersRepl.command":                               rate,
		"opcountersRepl.delete":                                rate,
		"opcountersRepl.getmore":                               rate,
		"opcountersRepl.insert":                                rate,
		"opcountersRepl.query":                                 rate,
		"opcountersRepl.update":                                rate,
		"uptime":                                               gauge,
	}

	// replSetGetStatus metrics collected by default.
	// https://docs.mongodb.com/manual/reference/command/replSetGetStatus/
	replStatusMetrics = map[string]string{
		"replSet.health":         gauge,
		"replSet.replicationLag": gauge,
		"replSet.state":          gauge,
	}

	// dbStats metrics collected by default.
	// https://docs.mongodb.com/manual/reference/command/dbStats/
	dbStatsMetrics = map[string]string{
		"avgObjSize":  gauge,
		"collections": gauge,
		"dataSize":    gauge,
		"fileSize":    gauge,
		"indexes":     gauge,
		"indexSize":   gauge,
		"nsSizeMB":    gauge,
		"numExtents":  gauge,
		"objects":     gauge,
		"storageSize": gauge,
	}

	// Journaling-related operations and performance report.
	// https://docs.mongodb.org/manual/reference/command/serverStatus/#serverStatus.dur
	durabilityMetrics = map[string]string{
		"dur.commits":                 gauge,
		"dur.commitsInWriteLock":      gauge,
		"dur.compression":             gauge,
		"dur.earlyCommits":            gauge,
		"dur.journaledMB":             gauge,
		"dur.timeMs.dt":               gauge,
		"dur.timeMs.prepLogBuffer":    gauge,
		"dur.timeMs.remapPrivateView": gauge,
		"dur.timeMs.writeToDataFiles": gauge,
		"dur.timeMs.writeToJournal":   gauge,
		"dur.writeToDataFilesMB":      gauge,

		// Required version > 3.0.0
		"dur.timeMs.commits":            gauge,
		"dur.timeMs.commitsInWriteLock": gauge,
	}

	// ServerStatus use of database commands report.
	// Required version > 3.0.0.
	// https://docs.mongodb.org/manual/reference/command/serverStatus/#serverStatus.metrics.commands
	commandsMetrics = map[string]string{
		"metrics.commands.count.failed":         rate,
		"metrics.commands.count.total":          gauge,
		"metrics.commands.createIndexes.failed": rate,
		"metrics.commands.createIndexes.total":  gauge,
		"metrics.commands.delete.failed":        rate,
		"metrics.commands.delete.total":         gauge,
		"metrics.commands.eval.failed":          rate,
		"metrics.commands.eval.total":           gauge,
		"metrics.commands.findAndModify.failed": rate,
		"metrics.commands.findAndModify.total":  gauge,
		"metrics.commands.insert.failed":        rate,
		"metrics.commands.insert.total":         gauge,
		"metrics.commands.update.failed":        rate,
		"metrics.commands.update.total":         gauge,
	}

	// ServerStatus locks report.
	// Required version > 3.0.0.
	// https://docs.mongodb.org/manual/reference/command/serverStatus/#server-status-locks
	locksMetrics = map[string]string{
		"locks.Collection.acquireCount.R":           rate,
		"locks.Collection.acquireCount.r":           rate,
		"locks.Collection.acquireCount.W":           rate,
		"locks.Collection.acquireCount.w":           rate,
		"locks.Collection.acquireWaitCount.R":       rate,
		"locks.Collection.acquireWaitCount.W":       rate,
		"locks.Collection.timeAcquiringMicros.R":    rate,
		"locks.Collection.timeAcquiringMicros.W":    rate,
		"locks.Database.acquireCount.r":             rate,
		"locks.Database.acquireCount.R":             rate,
		"locks.Database.acquireCount.w":             rate,
		"locks.Database.acquireCount.W":             rate,
		"locks.Database.acquireWaitCount.r":         rate,
		"locks.Database.acquireWaitCount.R":         rate,
		"locks.Database.acquireWaitCount.w":         rate,
		"locks.Database.acquireWaitCount.W":         rate,
		"locks.Database.timeAcquiringMicros.r":      rate,
		"locks.Database.timeAcquiringMicros.R":      rate,
		"locks.Database.timeAcquiringMicros.w":      rate,
		"locks.Database.timeAcquiringMicros.W":      rate,
		"locks.Global.acquireCount.r":               rate,
		"locks.Global.acquireCount.R":               rate,
		"locks.Global.acquireCount.w":               rate,
		"locks.Global.acquireCount.W":               rate,
		"locks.Global.acquireWaitCount.r":           rate,
		"locks.Global.acquireWaitCount.R":           rate,
		"locks.Global.acquireWaitCount.w":           rate,
		"locks.Global.acquireWaitCount.W":           rate,
		"locks.Global.timeAcquiringMicros.r":        rate,
		"locks.Global.timeAcquiringMicros.R":        rate,
		"locks.Global.timeAcquiringMicros.w":        rate,
		"locks.Global.timeAcquiringMicros.W":        rate,
		"locks.Metadata.acquireCount.R":             rate,
		"locks.Metadata.acquireCount.W":             rate,
		"locks.MMAPV1Journal.acquireCount.r":        rate,
		"locks.MMAPV1Journal.acquireCount.w":        rate,
		"locks.MMAPV1Journal.acquireWaitCount.r":    rate,
		"locks.MMAPV1Journal.acquireWaitCount.w":    rate,
		"locks.MMAPV1Journal.timeAcquiringMicros.r": rate,
		"locks.MMAPV1Journal.timeAcquiringMicros.w": rate,
		"locks.oplog.acquireCount.R":                rate,
		"locks.oplog.acquireCount.w":                rate,
		"locks.oplog.acquireWaitCount.R":            rate,
		"locks.oplog.acquireWaitCount.w":            rate,
		"locks.oplog.timeAcquiringMicros.R":         rate,
		"locks.oplog.timeAcquiringMicros.w":         rate,
	}

	// TCMalloc memory allocator report.
	tcmallocMetrics = map[string]string{
		"tcmalloc.generic.current_allocated_bytes":           gauge,
		"tcmalloc.generic.heap_size":                         gauge,
		"tcmalloc.tcmalloc.aggressive_memory_decommit":       gauge,
		"tcmalloc.tcmalloc.central_cache_free_bytes":         gauge,
		"tcmalloc.tcmalloc.current_total_thread_cache_bytes": gauge,
		"tcmalloc.tcmalloc.max_total_thread_cache_bytes":     gauge,
		"tcmalloc.tcmalloc.pageheap_free_bytes":              gauge,
		"tcmalloc.tcmalloc.pageheap_unmapped_bytes":          gauge,
		"tcmalloc.tcmalloc.thread_cache_free_bytes":          gauge,
		"tcmalloc.tcmalloc.transfer_cache_free_bytes":        gauge,
	}

	// WiredTiger storage engine.
	wiredtigerMetrics = map[string]string{
		"wiredTiger.cache.bytes currently in the cache":                                 gauge,
		"wiredTiger.cache.failed eviction of pages that exceeded the in-memory maximum": rate,
		"wiredTiger.cache.in-memory page splits":                                        gauge,
		"wiredTiger.cache.maximum bytes configured":                                     gauge,
		"wiredTiger.cache.maximum page size at eviction":                                gauge,
		"wiredTiger.cache.pages currently held in the cache":                            gauge,
		"wiredTiger.cache.pages evicted because they exceeded the in-memory maximum":    rate,
		"wiredTiger.cache.pages evicted by application threads":                         rate,
		"wiredTiger.concurrentTransactions.read.available":                              gauge,
		"wiredTiger.concurrentTransactions.read.out":                                    gauge,
		"wiredTiger.concurrentTransactions.read.totalTickets":                           gauge,
		"wiredTiger.concurrentTransactions.write.available":                             gauge,
		"wiredTiger.concurrentTransactions.write.out":                                   gauge,
		"wiredTiger.concurrentTransactions.write.totalTickets":                          gauge,
	}

	// Usage statistics for each collection.
	// https://docs.mongodb.org/v3.0/reference/command/top/
	topMetrics = map[string]string{
		"commands.count":  gauge,
		"commands.time":   gauge,
		"getmore.count":   gauge,
		"getmore.time":    gauge,
		"insert.count":    gauge,
		"insert.time":     gauge,
		"queries.count":   gauge,
		"queries.time":    gauge,
		"readLock.count":  gauge,
		"readLock.time":   gauge,
		"remove.count":    gauge,
		"remove.time":     gauge,
		"total.count":     gauge,
		"total.time":      gauge,
		"update.count":    gauge,
		"update.time":     gauge,
		"writeLock.count": gauge,
		"writeLock.time":  gauge,
	}

	// Mapping for case-sensitive metric name suffixes.
	// https://docs.mongodb.org/manual/reference/command/serverStatus/#server-status-locks
	caseSensitiveMetricNameSuffixes = map[string]string{
		`(.*)\.R$`: ".shared",
		`(.*)\.r$`: ".intent_shared",
		`(.*)\.W$`: ".exclusive",
		`(.*)\.w$`: ".intent_exclusive",
	}

	// Associates with the metric list to collect.
	availableMetrics = map[string]map[string]string{
		"durability":       durabilityMetrics,
		"locks":            locksMetrics,
		"metrics.commands": commandsMetrics,
		"tcmalloc":         tcmallocMetrics,
		"wiredtiger":       wiredtigerMetrics,
		"top":              topMetrics,
	}
)

// Check XXX
func (m *MongoDB) Check(agg metric.Aggregator) error {
	session := MongoSession{}
	var err error
	if m.Timeout > 0 {
		session.Session, err = mgo.DialWithTimeout(m.Server, time.Duration(m.Timeout)*time.Second)
	} else {
		session.Session, err = mgo.Dial(m.Server)
	}
	if err != nil {
		return err
	}
	defer session.Close()

	u, err := url.Parse(m.Server)
	if err != nil {
		return fmt.Errorf("Unable to parse to address '%s': %s", m.Server, err)
	}
	tags := append(m.Tags, "server:"+u.Host)

	err = m.collectMetrics(session, tags, agg)
	if err != nil {
		return err
	}
	return nil
}

func (m *MongoDB) collectMetrics(session Session, tags []string, agg metric.Aggregator) error {
	var err error
	// tags will be changed if we can get replSetGetStatus.
	err = m.collectReplSetGetStatus(session, &tags, agg)
	if err != nil {
		return err
	}

	err = m.collectServerStatus(session, tags, agg)
	if err != nil {
		return err
	}

	err = m.collectDBStats(session, tags, agg)
	if err != nil {
		return err
	}

	return nil
}

func (m *MongoDB) collectReplSetGetStatus(session Session, tags *[]string, agg metric.Aggregator) error {
	replStatus := ReplSetStatus{}
	if err := session.Run("replSetGetStatus", &replStatus); err == nil {
		replSet := bson.M{}
		var primary, current ReplSetMember

		// find nodes: master and current node (ourself)
		for _, member := range replStatus.Members {
			if member.Self {
				current = member
			}
			if member.State == 1 {
				primary = member
				replSet["replicationLag"] = 0
			}
		}

		if current.State == 2 {
			// OptimeDate.Unix() type is int64
			lag := primary.OptimeDate.Unix() - current.OptimeDate.Unix()
			if lag < 0 {
				replSet["replicationLag"] = 0
			} else {
				replSet["replicationLag"] = lag
			}
		}

		replSet["health"] = current.Health
		replSet["state"] = replStatus.MyState
		*tags = append(*tags, "replset_state:"+replsetStates[replStatus.MyState])

		stats := bson.M{}
		stats["replSet"] = replSet
		m.submitMetrics(stats, replStatusMetrics, *tags, agg)
	}
	return nil
}

func (m *MongoDB) collectServerStatus(session Session, tags []string, agg metric.Aggregator) error {
	serverStatus := bson.M{}
	if err := session.Run("serverStatus", &serverStatus); err != nil {
		return err
	}
	m.submitMetrics(serverStatus, serverStatusMetrics, tags, agg)

	for _, option := range m.AdditionalMetrics {
		if val, ok := availableMetrics[option]; !ok {
			log.Warnf("Failed to extend the list of metrics to collect: unrecognized %s option", option)
		} else {
			log.Debugf("Adding `%s` corresponding metrics to the list of metrics to collect.", option)

			if reflect.DeepEqual(val, topMetrics) {
				top := Top{}
				if err := session.Run("top", &top); err != nil {
					log.Error(err)
					continue
				}

				for k, stats := range top.Totals {
					if !strings.Contains(k, ".") {
						continue
					}

					// configure tags for db name and collection name
					split := strings.SplitN(k, ".", 2)
					dbname, collname := split[0], split[1]
					topTags := append(tags, "db:"+dbname, "collection:"+collname)
					m.submitMetrics(stats, val, topTags, agg)
				}
			} else {
				m.submitMetrics(serverStatus, val, tags, agg)
			}
		}
	}
	return nil
}

func (m *MongoDB) collectDBStats(session Session, tags []string, agg metric.Aggregator) error {
	names, err := session.DatabaseNames()
	if err != nil {
		log.Errorf("Failed to get database names, %s", err.Error())
	}
	for _, name := range names {
		dbStats := bson.M{}
		err = session.DB(name).Run(bson.D{
			{
				Name:  "dbStats",
				Value: 1,
			},
		}, &dbStats)
		if err != nil {
			return err
		}
		dbTags := append(tags, "cluster:db:"+name)
		m.submitMetrics(dbStats, dbStatsMetrics, dbTags, agg)
	}
	return nil
}

func (m *MongoDB) submitMetrics(stats bson.M, metrics map[string]string, tags []string, agg metric.Aggregator) {
	for k, v := range metrics {
		val, err := getFloatValue(stats, strings.Split(k, "."))
		if err != nil {
			// log.Warnf("Cannot fetch metric %s: %s", k, err)
			continue
		}

		var metricName string
		if reflect.DeepEqual(metrics, topMetrics) {
			metricName = normalize(k, v, "usage")
		} else if reflect.DeepEqual(metrics, dbStatsMetrics) {
			metricName = normalize(k, v, "stats")
		} else if reflect.DeepEqual(metrics, wiredtigerMetrics) {
			metricName = normalize(strings.Replace(k, " ", "_", -1), v, "")
		} else {
			metricName = normalize(k, v, "")
		}
		agg.Add(v, metric.NewMetric(metricName, val, tags))
	}
}

func getFloatValue(s map[string]interface{}, keys []string) (float64, error) {
	var val float64
	sm := s
	var err error
	for i, k := range keys {
		if i+1 < len(keys) {
			switch sm[k].(type) {
			case bson.M:
				sm = sm[k].(bson.M)
			// Just to be compatible with the TestCase.
			case map[string]interface{}:
				sm = sm[k].(map[string]interface{})
			default:
				return 0, fmt.Errorf("Cannot handle as a hash for %s", k)
			}
		} else {
			val, err = strconv.ParseFloat(fmt.Sprint(sm[k]), 64)
			if err != nil {
				return 0, err
			}
		}
	}

	return val, nil
}

func normalize(metricName, metricType, prefix string) string {
	// Replace case-sensitive metric name characters, normalize the metric name,
	// prefix and suffix according to its type.
	var metricPrefix, metricSuffix string
	if prefix == "" {
		metricPrefix = "mongodb."
	} else {
		metricPrefix = fmt.Sprintf("mongodb.%s.", prefix)
	}

	if metricType == rate {
		metricSuffix = "ps"
	}

	// Replace case-sensitive metric name characters
	for pattern, repl := range caseSensitiveMetricNameSuffixes {
		re := regexp.MustCompile(pattern)
		metricName = re.ReplaceAllString(metricName, fmt.Sprintf("${1}%s", repl))
	}

	// Normalize
	return metricPrefix + strings.ToLower(metricName) + metricSuffix
}

func init() {
	collector.Add("mongo", NewMongoDB)
}
