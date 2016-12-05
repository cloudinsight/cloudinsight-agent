package postgres

import (
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
	countStatResult = sqlmock.NewRows([]string{"schemaname", "count"})
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

	mock.ExpectQuery("^SELECT (.+) FROM pg_stat_database (.+)$").WillReturnRows(dbStatResult)
	mock.ExpectQuery("^SELECT (.+) FROM pg_stat_bgwriter$").WillReturnRows(bgwStatResult)
	mock.ExpectQuery("SELECT(.+)WHERE (.+) pg_is_in_recovery(.+)$").WillReturnRows(replStatResult)
	mock.ExpectQuery("SELECT (.+) FROM pg_stat_database, max_con$").WillReturnRows(connStatResult)
	mock.ExpectQuery("^SELECT (.+) FROM pg_locks (.+)$").WillReturnRows(lockStatResult)
	mock.ExpectQuery("^SELECT (.+) FROM pg_stat_user_tables GROUP BY schemaname$").WillReturnRows(countStatResult)

	p := &Postgres{
		Tags:    []string{"service:postgres"},
		version: "9.3.15",
	}
	metricC := make(chan metric.Metric, 100)
	defer close(metricC)
	agg := testutil.MockAggregator(metricC)

	err = p.collectMetrics(db, agg)
	require.NoError(t, err)
	agg.Flush()
	expectedMetrics := 2
	assert.Len(t, metricC, expectedMetrics)

	metrics := make([]metric.Metric, expectedMetrics)
	for i := 0; i < expectedMetrics; i++ {
		metrics[i] = <-metricC
	}

	fields := map[string]float64{
		"postgresql.connections":   float64(0),
		"postgresql.database_size": float64(6696724),
	}
	tags := []string{"service:postgres", "db:exampledb"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}
