package postgres

import (
	"sort"
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var (
	versionResult = sqlmock.NewRows([]string{"server_version"}).
			AddRow("9.3.15")
	dbStatResult = sqlmock.NewRows([]string{"datname", "numbackends", "xact_commit", "xact_rollback",
		"blks_read", "blks_hit", "tup_returned", "tup_fetched", "tup_inserted", "tup_updated", "tup_deleted",
		"temp_files", "temp_bytes", "deadlocks", "pg_database_size"}).
		AddRow("exampledb", 0, 321, 0, 180, 10393, 144823, 2553, 15, 1, 0, 0, 0, 0, 6696724)
	bgwStatResult = sqlmock.NewRows([]string{"checkpoints_timed", "checkpoints_req", "checkpoint_write_time",
		"checkpoint_sync_time", "buffers_checkpoint", "buffers_clean", "maxwritten_clean", "buffers_backend",
		"buffers_backend_fsync", "buffers_alloc"}).
		AddRow(519, 2, 1612, 303, 29, 0, 0, 1, 0, 1237)
	replStatResult = sqlmock.NewRows([]string{"replication_delay", "replication_delay_bytes"})
	connStatResult = sqlmock.NewRows([]string{"max_connections", "pct_connections"}).
			AddRow(100, 0.01)
	lockStatResult  = sqlmock.NewRows([]string{"mode", "relname", "lock_count"})
	countStatResult = sqlmock.NewRows([]string{"schemaname", "table_count"}).
			AddRow("public", 1)
	relStatResult = sqlmock.NewRows([]string{"relname", "schemaname", "seq_tup_read", "n_tup_del", "n_live_tup",
		"n_tup_hot_upd", "n_dead_tup", "seq_scan", "idx_scan", "idx_tup_fetch", "n_tup_ins", "n_tup_upd"}).
		AddRow("persons", "public", 2, 0, 2, 0, 0, 1, nil, nil, 2, 0)
	idxStatResult  = sqlmock.NewRows([]string{"relname", "schemaname", "indexrelname", "idx_scan", "idx_tup_read", "idx_tup_fetch"})
	sizeStatResult = sqlmock.NewRows([]string{"relname", "total_size", "table_size", "index_size"}).
			AddRow("persons", 16384, 16384, 0)
	statioStatResult = sqlmock.NewRows([]string{"relname", "schemaname", "toast_blks_hit", "tidx_blks_read", "tidx_blks_hit",
		"heap_blks_read", "heap_blks_hit", "idx_blks_read", "idx_blks_hit", "toast_blks_read"}).
		AddRow("persons", "public", 0, 0, 0, 1, 2, nil, nil, 0)
	customStatResult = sqlmock.NewRows([]string{"datname", "numbackends"}).
				AddRow("exampledb", 1)
)

func TestGetVersion(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery(versionQuery).WillReturnRows(versionResult)
	p := &Postgres{}
	version, err := p.getVersion(db)
	assert.NoError(t, err)
	assert.Equal(t, "9.3.15", version)

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestCollectMetrics(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("^SELECT (.+) FROM pg_stat_database WHERE datname not ilike (.+)$").WillReturnRows(dbStatResult)
	mock.ExpectQuery("^SELECT (.+) FROM pg_stat_bgwriter$").WillReturnRows(bgwStatResult)
	mock.ExpectQuery("SELECT(.+)WHERE (.+) pg_is_in_recovery(.+)$").WillReturnRows(replStatResult)
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database, max_con$").WillReturnRows(connStatResult)
	mock.ExpectQuery("^SELECT (.+) FROM pg_locks (.+)$").WillReturnRows(lockStatResult)
	mock.ExpectQuery("^SELECT (.+) FROM pg_stat_user_tables GROUP BY schemaname$").WillReturnRows(countStatResult)
	mock.ExpectQuery("^SELECT relname, schemaname,(.+) FROM pg_stat_user_tables WHERE relname = ANY(.+)$").WillReturnRows(relStatResult)
	mock.ExpectQuery("^SELECT relname, schemaname, indexrelname,(.+) FROM pg_stat_user_indexes WHERE relname = ANY(.+)$").WillReturnRows(idxStatResult)
	mock.ExpectQuery("^SELECT relname,(.+) FROM pg_class C (.+)$").WillReturnRows(sizeStatResult)
	mock.ExpectQuery("^SELECT relname, schemaname,(.+) FROM pg_statio_user_tables WHERE relname = ANY(.+)$").WillReturnRows(statioStatResult)
	mock.ExpectQuery("^SELECT datname, (.+) FROM pg_stat_database WHERE datname='exampledb'(.+)$").WillReturnRows(customStatResult)

	p := &Postgres{
		Address: "host=localhost sslmode=disable",
		Tags:    []string{"service:postgres"},
		Relations: []relationConfig{
			{
				RelationName: "my_table",
				Schemas:      []string{},
			},
			{
				RelationName: "my_other_table",
				Schemas:      []string{"public", "proc"},
			},
		},
		CustomMetrics: []metricSchema{
			{
				Descriptors: []tagField{
					{
						Name:  "datname",
						Alias: "customdb",
					},
				},
				Metrics: map[string]metric.Field{
					"numbackends": {"postgresql.custom.numbackends", gauge},
				},
				Query:    "SELECT datname, %s FROM pg_stat_database WHERE datname='exampledb' LIMIT(1)",
				Relation: false,
			},
		},
		version: "9.3.15",
	}
	metricC := make(chan metric.Metric, 100)
	defer close(metricC)
	agg := testutil.MockAggregator(metricC)

	err = p.formatAddress()
	require.NoError(t, err)
	err = p.collectMetrics(db, agg)
	require.NoError(t, err)
	agg.Flush()
	expectedMetrics := 12
	require.Len(t, metricC, expectedMetrics)

	metrics := make([]metric.Metric, expectedMetrics)
	for i := 0; i < expectedMetrics; i++ {
		metrics[i] = <-metricC
	}

	// dbMetrics
	fields := map[string]float64{
		"postgresql.connections":   float64(0),
		"postgresql.database_size": float64(6696724),
	}
	tags := []string{"service:postgres", "db:exampledb"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	// relMetrics
	fields = map[string]float64{
		"postgresql.live_rows": float64(2),
		"postgresql.dead_rows": float64(0),
	}
	tags = []string{"service:postgres", "server:host=localhost sslmode=disable", "table:persons", "schema:public"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	// sizeMetrics
	fields = map[string]float64{
		"postgresql.table_size": float64(16384),
		"postgresql.index_size": float64(0),
		"postgresql.total_size": float64(16384),
	}
	tags = []string{"service:postgres", "server:host=localhost sslmode=disable", "table:persons"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	// countMetrics
	fields = map[string]float64{
		"postgresql.table.count": float64(1),
	}
	tags = []string{"service:postgres", "schema:public"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	fields = map[string]float64{
		// "postgresql.locks":                     float64(0),
		// "postgresql.replication_delay":         float64(0),
		// "postgresql.replication_delay_bytes":   float64(0),
		"postgresql.max_connections":           float64(100),
		"postgresql.percent_usage_connections": float64(0.01),
		"postgresql.db.count":                  float64(1),
	}
	tags = []string{"service:postgres"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestFormatAddress(t *testing.T) {
	p := &Postgres{
		Address: "postgres://postgres:postgres@localhost/postgres?sslmode=disable",
	}
	err := p.formatAddress()
	assert.NoError(t, err)
	expectedAddr := "dbname=postgres host=localhost sslmode=disable user=postgres"
	assert.Equal(t, expectedAddr, p.formattedAddress)
}

func TestGetMapKeys(t *testing.T) {
	metrics := map[string]metric.Field{
		"test":           {"postgresql.test", gauge},
		"test_rate":      {"postgresql.test_rate", rate},
		"test_monotonic": {"postgresql.test_monotonic", monotonic},
	}
	keys := getMapKeys(metrics)
	sort.Strings(keys)
	assert.Equal(t, []string{"test", "test_monotonic", "test_rate"}, keys)
}

func TestGetMetricsWithAsCondition(t *testing.T) {
	metrics := map[string]metric.Field{
		"pg_database_size(datname) as pg_database_size": {"postgresql.database_size", gauge},
	}
	expectedMetrics := map[string]metric.Field{
		"pg_database_size": {"postgresql.database_size", gauge},
	}
	assert.Equal(t, expectedMetrics, getMetricsWithAsCondition(metrics))
}
