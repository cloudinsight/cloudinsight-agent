package mongodb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"gopkg.in/mgo.v2/bson"

	"github.com/cloudinsight/cloudinsight-agent/common"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/stretchr/testify/require"
)

// MockSession satisfies Session and act as a mock of *mgo.Session.
type MockSession struct{}

// NewMockSession mock NewSession.
func NewMockSession() Session {
	return &MockSession{}
}

// Run mocks mgo.Session.Run().
func (m *MockSession) Run(cmd interface{}, result interface{}) error {
	return m.DB("admin").Run(cmd, result)
}

// DB mocks mgo.Session.DB().
func (m *MockSession) DB(name string) DataLayer {
	return &MockDatabase{}
}

// DatabaseNames mocks mgo.Session.DatabaseNames().
func (m *MockSession) DatabaseNames() ([]string, error) {
	return []string{"local"}, nil
}

// Close mocks mgo.Session.Close().
func (m *MockSession) Close() {}

type MockDatabase struct{}

// Run mocks mgo.Session.Run().
func (m *MockDatabase) Run(cmd interface{}, result interface{}) error {
	var data []byte
	var err error
	switch val := cmd.(type) {
	case string:
		data, err = ioutil.ReadFile(fmt.Sprintf("testdata/%s.json", val))
	case bson.D:
		data, err = ioutil.ReadFile(fmt.Sprintf("testdata/%s.json", val[0].Name))
	}
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &result)
	if err != nil {
		return err
	}
	return nil
}

func TestCollectMetrics(t *testing.T) {
	var err error
	session := NewMockSession()
	defer session.Close()

	m := &MongoDB{
		Tags:              []string{"service:mongodb"},
		AdditionalMetrics: []string{"durability", "locks", "metrics.commands", "tcmalloc", "top", "wiredtiger"},
	}

	metricC := make(chan metric.Metric, 1000)
	defer close(metricC)
	agg := testutil.MockAggregator(metricC)

	err = m.collectMetrics(session, m.Tags, agg)
	require.NoError(t, err)
	agg.Flush()
	expectedMetrics := 215
	require.Len(t, metricC, expectedMetrics)

	metrics := make([]metric.Metric, expectedMetrics)
	for i := 0; i < expectedMetrics; i++ {
		metrics[i] = <-metricC
	}

	fields := map[string]float64{
		// replset metrics
		"mongodb.replset.health":         1,
		"mongodb.replset.replicationlag": 0,
		"mongodb.replset.state":          1,

		"mongodb.backgroundflushing.average_ms":    1.5,
		"mongodb.backgroundflushing.last_ms":       2,
		"mongodb.backgroundflushing.total_ms":      3,
		"mongodb.connections.available":            419429,
		"mongodb.connections.current":              1,
		"mongodb.connections.totalcreated":         17,
		"mongodb.cursors.timedout":                 0,
		"mongodb.cursors.totalopen":                0,
		"mongodb.globallock.activeclients.readers": 0,
		"mongodb.globallock.activeclients.total":   12,
		"mongodb.globallock.activeclients.writers": 0,
		"mongodb.globallock.currentqueue.readers":  0,
		"mongodb.globallock.currentqueue.total":    0,
		"mongodb.globallock.currentqueue.writers":  0,
		// "mongodb.globallock.locktime":              0,
		"mongodb.globallock.totaltime": 136412000,
		// "mongodb.indexcounters.missratio":          0,
		"mongodb.mem.bits":                         64,
		"mongodb.mem.mapped":                       1119,
		"mongodb.mem.mappedwithjournal":            2238,
		"mongodb.mem.resident":                     92,
		"mongodb.mem.virtual":                      2343,
		"mongodb.metrics.cursor.open.notimeout":    0,
		"mongodb.metrics.cursor.open.pinned":       0,
		"mongodb.metrics.cursor.open.total":        0,
		"mongodb.metrics.repl.buffer.count":        0,
		"mongodb.metrics.repl.buffer.maxsizebytes": 268435456,
		"mongodb.metrics.repl.buffer.sizebytes":    0,
		"mongodb.uptime":                           137,

		// durability metrics
		"mongodb.dur.commits":                 30,
		"mongodb.dur.commitsinwritelock":      0,
		"mongodb.dur.compression":             0,
		"mongodb.dur.earlycommits":            0,
		"mongodb.dur.journaledmb":             0,
		"mongodb.dur.timems.dt":               3070,
		"mongodb.dur.timems.preplogbuffer":    0,
		"mongodb.dur.timems.remapprivateview": 0,
		"mongodb.dur.timems.writetodatafiles": 0,
		"mongodb.dur.timems.writetojournal":   0,
		"mongodb.dur.writetodatafilesmb":      0,

		// commands metrics
		// "mongodb.metrics.commands.count.total":         0,
		// "mongodb.metrics.commands.createindexes.total": 0,
		// "mongodb.metrics.commands.delete.total":        0,
		// "mongodb.metrics.commands.eval.total":          0,
		// "mongodb.metrics.commands.findandmodify.total": 0,
		// "mongodb.metrics.commands.insert.total":        0,
		// "mongodb.metrics.commands.update.total":        0,
	}
	tags := []string{"service:mongodb", "replset_state:primary"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	// top metrics
	fields = map[string]float64{
		"mongodb.usage.commands.count":  0,
		"mongodb.usage.commands.time":   0,
		"mongodb.usage.getmore.count":   0,
		"mongodb.usage.getmore.time":    0,
		"mongodb.usage.insert.count":    0,
		"mongodb.usage.insert.time":     0,
		"mongodb.usage.queries.count":   1,
		"mongodb.usage.queries.time":    43,
		"mongodb.usage.readlock.count":  1,
		"mongodb.usage.readlock.time":   43,
		"mongodb.usage.remove.count":    0,
		"mongodb.usage.remove.time":     0,
		"mongodb.usage.total.count":     1,
		"mongodb.usage.total.time":      43,
		"mongodb.usage.update.count":    0,
		"mongodb.usage.update.time":     0,
		"mongodb.usage.writelock.count": 0,
		"mongodb.usage.writelock.time":  0,
	}
	tags = []string{"service:mongodb", "replset_state:primary", "db:admin", "collection:system.roles"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	// top metrics
	fields = map[string]float64{
		"mongodb.usage.commands.count":  0,
		"mongodb.usage.commands.time":   0,
		"mongodb.usage.getmore.count":   0,
		"mongodb.usage.getmore.time":    0,
		"mongodb.usage.insert.count":    0,
		"mongodb.usage.insert.time":     0,
		"mongodb.usage.queries.count":   2,
		"mongodb.usage.queries.time":    19,
		"mongodb.usage.readlock.count":  1089,
		"mongodb.usage.readlock.time":   2432,
		"mongodb.usage.remove.count":    0,
		"mongodb.usage.remove.time":     0,
		"mongodb.usage.total.count":     1090,
		"mongodb.usage.total.time":      2433,
		"mongodb.usage.update.count":    0,
		"mongodb.usage.update.time":     0,
		"mongodb.usage.writelock.count": 1,
		"mongodb.usage.writelock.time":  1,
	}
	tags = []string{"service:mongodb", "replset_state:primary", "db:local", "collection:me"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	// dbstats metrics
	fields = map[string]float64{
		"mongodb.stats.avgobjsize":  257.2,
		"mongodb.stats.collections": 6,
		"mongodb.stats.datasize":    7716,
		"mongodb.stats.filesize":    1089994752,
		"mongodb.stats.indexes":     3,
		"mongodb.stats.indexsize":   24528,
		"mongodb.stats.nssizemb":    16,
		"mongodb.stats.numextents":  8,
		"mongodb.stats.objects":     30,
		"mongodb.stats.storagesize": 1048608752,
	}
	tags = []string{"service:mongodb", "replset_state:primary", "cluster:db:local"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}
}
