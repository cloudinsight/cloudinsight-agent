package mysql

import (
	"database/sql"
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var (
	globalStatusResult = sqlmock.NewRows([]string{"key", "val"}).
		// Command Metrics
		AddRow("Slow_queries", 0).
		AddRow("Questions", 1791).
		AddRow("Queries", 1793).
		AddRow("Com_select", 754).
		AddRow("Com_insert", 0).
		AddRow("Com_update", 0).
		AddRow("Com_delete", 0).
		AddRow("Com_replace", 0).
		AddRow("Com_load", 0).
		AddRow("Com_insert_select", 0).
		AddRow("Com_update_multi", 0).
		AddRow("Com_delete_multi", 0).
		AddRow("Com_replace_select", 0).
		// Connection Metrics
		AddRow("Connections", 6502).
		AddRow("Max_used_connections", 3).
		AddRow("Aborted_clients", 1).
		AddRow("Aborted_connects", 0).
		// Table Cache Metrics
		AddRow("Open_files", 48).
		AddRow("Open_tables", 63).
		// Network Metrics
		AddRow("Bytes_sent", 139980303).
		AddRow("Bytes_received", 5698867).
		// Query Cache Metrics
		AddRow("Qcache_hits", 0).
		AddRow("Qcache_inserts", 0).
		AddRow("Qcache_lowmem_prunes", 0).
		// Table Lock Metrics
		AddRow("Table_locks_waited", 0).
		// Temporary Table Metrics
		AddRow("Created_tmp_tables", 454633).
		AddRow("Created_tmp_disk_tables", 106664).
		AddRow("Created_tmp_files", 6).
		// Thread Metrics
		AddRow("Threads_connected", 1).
		AddRow("Threads_running", 1).
		// MyISAM Metrics
		AddRow("Key_blocks_not_flushed", 0).
		AddRow("Key_blocks_unused", 13396).
		AddRow("Key_blocks_used", 0).
		AddRow("Key_read_requests", 0).
		AddRow("Key_reads", 0).
		AddRow("Key_write_requests", 0).
		AddRow("Key_writes", 0).
		// InnoDB metrics
		AddRow("Innodb_data_reads", 454).
		AddRow("Innodb_data_writes", 3).
		AddRow("Innodb_data_pending_fsyncs", 0).
		AddRow("Innodb_data_pending_reads", 0).
		AddRow("Innodb_data_pending_writes", 0).
		AddRow("Innodb_os_log_fsyncs", 3).
		AddRow("Innodb_row_lock_waits", 0).
		AddRow("Innodb_row_lock_time", 0).
		AddRow("Innodb_row_lock_current_waits", 0).
		AddRow("Innodb_buffer_pool_bytes_dirty", 0).
		AddRow("Innodb_buffer_pool_read_requests", 8788).
		AddRow("Innodb_buffer_pool_reads", 444).
		AddRow("Innodb_os_log_pending_fsyncs", 0).
		AddRow("Innodb_os_log_pending_writes", 0).
		AddRow("Innodb_page_size", 16384).
		// Additional Vars
		AddRow("Binlog_cache_disk_use", 0).
		AddRow("Binlog_cache_use", 0).
		AddRow("Handler_commit", 0).
		AddRow("Handler_delete", 0).
		AddRow("Handler_prepare", 0).
		AddRow("Handler_read_first", 5).
		AddRow("Handler_read_key", 9377).
		AddRow("Handler_read_next", 0).
		AddRow("Handler_read_prev", 0).
		AddRow("Handler_read_rnd", 857).
		AddRow("Handler_read_rnd_next", 658751).
		AddRow("Handler_rollback", 0).
		AddRow("Handler_update", 7848).
		AddRow("Handler_write", 975710).
		AddRow("Opened_tables", 63).
		AddRow("Qcache_total_blocks", 1).
		AddRow("Qcache_free_blocks", 1).
		AddRow("Qcache_free_memory", 16759696).
		AddRow("Qcache_not_cached", 3129).
		AddRow("Qcache_queries_in_cache", 0).
		AddRow("Select_full_join", 673).
		AddRow("Select_full_range_join", 0).
		AddRow("Select_range", 0).
		AddRow("Select_range_check", 0).
		AddRow("Select_scan", 4523).
		AddRow("Sort_merge_passes", 0).
		AddRow("Sort_range", 0).
		AddRow("Sort_rows", 857).
		AddRow("Sort_scan", 857).
		AddRow("Table_locks_immediate", 62).
		// AddRow("Table_locks_immediate_rate", 0).
		AddRow("Threads_cached", 1).
		AddRow("Threads_created", 2).
		AddRow("Table_open_cache_hits", 24851).
		AddRow("Table_open_cache_misses", 85).
		// Galera Vars
		AddRow("wsrep_cluster_size", 0).
		AddRow("wsrep_local_recv_queue_avg", 0).
		AddRow("wsrep_flow_control_paused", 0).
		AddRow("wsrep_cert_deps_distance", 0).
		AddRow("wsrep_local_send_queue_avg", 0)

	globalVariablesResult = sqlmock.NewRows([]string{"key", "val"}).
				AddRow("key_buffer_size", 16777216).
				AddRow("key_cache_block_size", 1024).
				AddRow("log_bin", "ON").
				AddRow("max_connections", 151).
				AddRow("performance_schema", "ON").
				AddRow("query_cache_size", 16777216).
				AddRow("table_open_cache", 400).
				AddRow("thread_cache_size", 8)

	binaryLogsResult = sqlmock.NewRows([]string{"Log_name", "File_size"}).
				AddRow("mysql-bin.000001", 107)

	engineResult = sqlmock.NewRows([]string{"engine"}).AddRow("InnoDB")

	tableSchemaResult = sqlmock.NewRows([]string{"table_schema", "total_mb"}).
				AddRow("information_schema", 0.00878906)

	stubInnodbStatus55 = `
=====================================
161125 18:50:11 INNODB MONITOR OUTPUT
=====================================
Per second averages calculated from the last 11 seconds
-----------------
BACKGROUND THREAD
-----------------
srv_master_thread loops: 1 1_second, 1 sleeps, 0 10_second, 1 background, 1 flush
srv_master_thread log flush and writes: 1
----------
SEMAPHORES
----------
OS WAIT ARRAY INFO: reservation count 3, signal count 3
Mutex spin waits 2, rounds 31, OS waits 1
RW-shared spins 2, rounds 60, OS waits 2
RW-excl spins 0, rounds 0, OS waits 0
Spin rounds per wait: 15.50 mutex, 30.00 RW-shared, 0.00 RW-excl
------------
TRANSACTIONS
------------
Trx id counter 2000
Purge done for trx's n:o < 1E0C undo n:o < 0
History list length 324
LIST OF TRANSACTIONS FOR EACH SESSION:
---TRANSACTION 0, not started
MySQL thread id 3610, OS thread handle 0x7fd1881d5700, query id 27654 localhost oneapm
SHOW /*!50000 ENGINE*/ INNODB STATUS
--------
FILE I/O
--------
I/O thread 0 state: waiting for completed aio requests (insert buffer thread)
I/O thread 1 state: waiting for completed aio requests (log thread)
I/O thread 2 state: waiting for completed aio requests (read thread)
I/O thread 3 state: waiting for completed aio requests (read thread)
I/O thread 4 state: waiting for completed aio requests (read thread)
I/O thread 5 state: waiting for completed aio requests (read thread)
I/O thread 6 state: waiting for completed aio requests (write thread)
I/O thread 7 state: waiting for completed aio requests (write thread)
I/O thread 8 state: waiting for completed aio requests (write thread)
I/O thread 9 state: waiting for completed aio requests (write thread)
Pending normal aio reads: 0 [0, 0, 0, 0] , aio writes: 0 [0, 0, 0, 0] ,
 ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
Pending flushes (fsync) log: 0; buffer pool: 0
454 OS file reads, 3 OS file writes, 3 OS fsyncs
0.00 reads/s, 0 avg bytes/read, 0.00 writes/s, 0.00 fsyncs/s
-------------------------------------
INSERT BUFFER AND ADAPTIVE HASH INDEX
-------------------------------------
Ibuf: size 1, free list len 0, seg size 2, 0 merges
merged operations:
 insert 0, delete mark 0, delete 0
discarded operations:
 insert 0, delete mark 0, delete 0
Hash table size 276707, node heap has 0 buffer(s)
0.00 hash searches/s, 0.00 non-hash searches/s
---
LOG
---
Log sequence number 2744156
Log flushed up to   2744156
Last checkpoint at  2744156
0 pending log writes, 0 pending chkp writes
8 log i/o's done, 0.00 log i/o's/second
----------------------
BUFFER POOL AND MEMORY
----------------------
Total memory allocated 137363456; in additional pool allocated 0
Dictionary memory allocated 190396
Buffer pool size   8192
Free buffers       7749
Database pages     443
Old database pages 0
Modified db pages  0
Pending reads 0
Pending writes: LRU 0, flush list 0, single page 0
Pages made young 0, not young 0
0.00 youngs/s, 0.00 non-youngs/s
Pages read 443, created 0, written 0
0.00 reads/s, 0.00 creates/s, 0.00 writes/s
No buffer pool page gets since the last printout
Pages read ahead 0.00/s, evicted without access 0.00/s, Random read ahead 0.00/s
LRU len: 443, unzip_LRU len: 0
I/O sum[0]:cur[0], unzip sum[0]:cur[0]
--------------
ROW OPERATIONS
--------------
0 queries inside InnoDB, 0 queries in queue
1 read views open inside InnoDB
Main thread process no. 1326, id 140537613301504, state: waiting for server activity
Number of rows inserted 0, updated 0, deleted 0, read 0
0.00 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
----------------------------
END OF INNODB MONITOR OUTPUT
============================`

	stubInnodbStatus56 = `
=====================================
2016-11-25 19:36:16 7fd02cb4f700 INNODB MONITOR OUTPUT
=====================================
Per second averages calculated from the last 25 seconds
-----------------
BACKGROUND THREAD
-----------------
srv_master_thread loops: 552 srv_active, 0 srv_shutdown, 376657 srv_idle
srv_master_thread log flush and writes: 377209
----------
SEMAPHORES
----------
OS WAIT ARRAY INFO: reservation count 687
OS WAIT ARRAY INFO: signal count 687
Mutex spin waits 287, rounds 4777, OS waits 153
RW-shared spins 529, rounds 15870, OS waits 528
RW-excl spins 0, rounds 180, OS waits 6
Spin rounds per wait: 16.64 mutex, 30.00 RW-shared, 180.00 RW-excl
------------
TRANSACTIONS
------------
Trx id counter 373762
Purge done for trx's n:o < 373760 undo n:o < 0 state: running but idle
History list length 1313
LIST OF TRANSACTIONS FOR EACH SESSION:
---TRANSACTION 0, not started
MySQL thread id 1696, OS thread handle 0x7fd02cb4f700, query id 28309 localhost oneapm init
SHOW /*!50000 ENGINE*/ INNODB STATUS
--------
FILE I/O
--------
I/O thread 0 state: waiting for completed aio requests (insert buffer thread)
I/O thread 1 state: waiting for completed aio requests (log thread)
I/O thread 2 state: waiting for completed aio requests (read thread)
I/O thread 3 state: waiting for completed aio requests (read thread)
I/O thread 4 state: waiting for completed aio requests (read thread)
I/O thread 5 state: waiting for completed aio requests (read thread)
I/O thread 6 state: waiting for completed aio requests (write thread)
I/O thread 7 state: waiting for completed aio requests (write thread)
I/O thread 8 state: waiting for completed aio requests (write thread)
I/O thread 9 state: waiting for completed aio requests (write thread)
Pending normal aio reads: 0 [0, 0, 0, 0] , aio writes: 0 [0, 0, 0, 0] ,
 ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
Pending flushes (fsync) log: 0; buffer pool: 0
692 OS file reads, 6851 OS file writes, 3411 OS fsyncs
0.00 reads/s, 0 avg bytes/read, 0.00 writes/s, 0.00 fsyncs/s
-------------------------------------
INSERT BUFFER AND ADAPTIVE HASH INDEX
-------------------------------------
Ibuf: size 1, free list len 0, seg size 2, 0 merges
merged operations:
 insert 0, delete mark 0, delete 0
discarded operations:
 insert 0, delete mark 0, delete 0
Hash table size 276671, node heap has 4 buffer(s)
0.00 hash searches/s, 0.00 non-hash searches/s
---
LOG
---
Log sequence number 21288221
Log flushed up to   21288221
Pages flushed up to 21288221
Last checkpoint at  21288221
0 pending log writes, 0 pending chkp writes
1753 log i/o's done, 0.00 log i/o's/second
----------------------
BUFFER POOL AND MEMORY
----------------------
Total memory allocated 137363456; in additional pool allocated 0
Dictionary memory allocated 199117
Buffer pool size   8191
Free buffers       7669
Database pages     518
Old database pages 210
Modified db pages  0
Pending reads 0
Pending writes: LRU 0, flush list 0, single page 0
Pages made young 0, not young 0
0.00 youngs/s, 0.00 non-youngs/s
Pages read 507, created 11, written 4589
0.00 reads/s, 0.00 creates/s, 0.00 writes/s
No buffer pool page gets since the last printout
Pages read ahead 0.00/s, evicted without access 0.00/s, Random read ahead 0.00/s
LRU len: 518, unzip_LRU len: 0
I/O sum[0]:cur[0], unzip sum[0]:cur[0]
--------------
ROW OPERATIONS
--------------
0 queries inside InnoDB, 0 queries in queue
0 read views open inside InnoDB
Main thread process no. 1766, id 140532050577152, state: sleeping
Number of rows inserted 93, updated 645, deleted 27, read 362189
0.00 inserts/s, 0.00 updates/s, 0.00 deletes/s, 0.00 reads/s
----------------------------
END OF INNODB MONITOR OUTPUT
============================`

	innodbStatusResult = sqlmock.NewRows([]string{"type", "name", "status"}).AddRow("InnoDB", "", stubInnodbStatus55)
)

func TestCollectMetrics(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("^SHOW (.+) GLOBAL (.+) STATUS$").WillReturnRows(globalStatusResult)
	mock.ExpectQuery("SELECT engine FROM information_schema.ENGINES (.+)").WillReturnRows(engineResult)
	mock.ExpectQuery("^SHOW (.+) ENGINE (.+) INNODB STATUS$").WillReturnRows(innodbStatusResult)
	mock.ExpectQuery(globalVariablesQuery).WillReturnRows(globalVariablesResult)
	mock.ExpectQuery(binaryLogsQuery).WillReturnRows(binaryLogsResult)
	mock.ExpectQuery("SELECT table_schema, (.+)total_mb FROM information_schema.tables (.+)").WillReturnRows(tableSchemaResult)

	m := &MySQL{
		Tags: []string{"env:production"},
		Options: Options{
			Replication:             true,
			GaleraCluster:           true,
			ExtraStatusMetrics:      true,
			ExtraInnodbMetrics:      true,
			ExtraPerformanceMetrics: true,
			SchemaSizeMetrics:       true,
			DisableInnodbMetrics:    false,
		},
	}
	metricC := make(chan metric.Metric, 100)
	defer close(metricC)
	agg := testutil.MockAggregator(metricC)

	err = m.collectMetrics(db, agg)
	require.NoError(t, err)
	agg.Flush()
	expectedMetrics := 68
	require.Len(t, metricC, expectedMetrics)

	metrics := make([]metric.Metric, expectedMetrics)
	for i := 0; i < expectedMetrics; i++ {
		metrics[i] = <-metricC
	}

	fields := map[string]float64{
		"mysql.binlog.cache_disk_use":              float64(0),
		"mysql.binlog.cache_use":                   float64(0),
		"mysql.binlog.disk_use":                    float64(107),
		"mysql.innodb.buffer_pool_data":            float64(7258112),
		"mysql.innodb.buffer_pool_dirty":           float64(0),
		"mysql.innodb.buffer_pool_free":            float64(126959616),
		"mysql.innodb.buffer_pool_pages_data":      float64(443),
		"mysql.innodb.buffer_pool_pages_dirty":     float64(0),
		"mysql.innodb.buffer_pool_pages_free":      float64(7749),
		"mysql.innodb.buffer_pool_pages_total":     float64(8192),
		"mysql.innodb.buffer_pool_read_ahead_rnd":  float64(0),
		"mysql.innodb.buffer_pool_total":           float64(134217728),
		"mysql.innodb.buffer_pool_used":            float64(7258112),
		"mysql.innodb.buffer_pool_utilization":     float64(8192-7749) / float64(8192),
		"mysql.innodb.checkpoint_age":              float64(0),
		"mysql.innodb.current_transactions":        float64(1),
		"mysql.innodb.data_pending_fsyncs":         float64(0),
		"mysql.innodb.data_pending_reads":          float64(0),
		"mysql.innodb.data_pending_writes":         float64(0),
		"mysql.innodb.hash_index_cells_total":      float64(276707),
		"mysql.innodb.hash_index_cells_used":       float64(0),
		"mysql.innodb.history_list_length":         float64(324),
		"mysql.innodb.ibuf_free_list":              float64(0),
		"mysql.innodb.ibuf_segment_size":           float64(2),
		"mysql.innodb.ibuf_size":                   float64(1),
		"mysql.innodb.mem_additional_pool":         float64(0),
		"mysql.innodb.mem_total":                   float64(137363456),
		"mysql.innodb.os_log_pending_fsyncs":       float64(0),
		"mysql.innodb.os_log_pending_writes":       float64(0),
		"mysql.innodb.pending_aio_log_ios":         float64(0),
		"mysql.innodb.pending_aio_sync_ios":        float64(0),
		"mysql.innodb.pending_buffer_pool_flushes": float64(0),
		"mysql.innodb.pending_checkpoint_writes":   float64(0),
		"mysql.innodb.pending_ibuf_aio_reads":      float64(0),
		"mysql.innodb.pending_log_flushes":         float64(0),
		"mysql.innodb.pending_log_writes":          float64(0),
		"mysql.innodb.pending_normal_aio_reads":    float64(0),
		"mysql.innodb.pending_normal_aio_writes":   float64(0),
		"mysql.innodb.queries_inside":              float64(0),
		"mysql.innodb.queries_queued":              float64(0),
		"mysql.innodb.read_views":                  float64(1),
		"mysql.innodb.row_lock_current_waits":      float64(0),
		"mysql.myisam.key_buffer_bytes_unflushed":  float64(0),
		"mysql.myisam.key_buffer_bytes_used":       float64(0),
		"mysql.myisam.key_buffer_size":             float64(16777216),
		"mysql.net.max_connections":                float64(3),
		"mysql.net.max_connections_available":      float64(151),
		"mysql.performance.key_cache_utilization":  float64(1 - (float64(13396*1024) / float64(16777216))),
		"mysql.performance.open_files":             float64(48),
		"mysql.performance.open_tables":            float64(63),
		// "mysql.performance.qcache.utilization":      float64(0),
		"mysql.performance.qcache_free_blocks":      float64(1),
		"mysql.performance.qcache_free_memory":      float64(16759696),
		"mysql.performance.qcache_queries_in_cache": float64(0),
		"mysql.performance.qcache_size":             float64(16777216),
		"mysql.performance.qcache_total_blocks":     float64(1),
		"mysql.performance.table_locks_immediate":   float64(62),
		"mysql.performance.table_locks_waited":      float64(0),
		"mysql.performance.table_open_cache":        float64(400),
		"mysql.performance.thread_cache_size":       float64(8),
		"mysql.performance.threads_cached":          float64(1),
		"mysql.performance.threads_connected":       float64(1),
		"mysql.performance.threads_running":         float64(1),
		// "mysql.replication.slave_running":           float64(0),
	}
	tags := []string{"env:production"}
	for name, value := range fields {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags)
	}

	tags = []string{"schema:information_schema"}
	testutil.AssertContainsMetricWithTags(t, metrics, "mysql.info.schema.size", 0.00878906, tags)

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}

func TestParseInnodbStatus55(t *testing.T) {
	stat := make(map[string]float64)

	err := parseInnodbStatus(stubInnodbStatus55, &stat)
	assert.NoError(t, err)
	// Innodb Semaphores
	assert.EqualValues(t, stat["Innodb_mutex_spin_waits"], 2)
	assert.EqualValues(t, stat["Innodb_mutex_spin_rounds"], 31)
	assert.EqualValues(t, stat["Innodb_mutex_os_waits"], 1)
	assert.EqualValues(t, stat["Innodb_semaphore_waits"], 0)
	assert.EqualValues(t, stat["Innodb_mutex_spin_waits"], 2)
	// Innodb Transactions
	assert.EqualValues(t, stat["Innodb_history_list_length"], 324)
	assert.EqualValues(t, stat["Innodb_current_transactions"], 1)
	assert.EqualValues(t, stat["Innodb_active_transactions"], 0)
	assert.EqualValues(t, stat["Innodb_row_lock_time"], 0)
	assert.EqualValues(t, stat["Innodb_read_views"], 1)
	assert.EqualValues(t, stat["Innodb_tables_in_use"], 0)
	assert.EqualValues(t, stat["Innodb_locked_tables"], 0)
	assert.EqualValues(t, stat["Innodb_lock_structs"], 0)
	assert.EqualValues(t, stat["Innodb_locked_transactions"], 0)
	// File I/O
	assert.EqualValues(t, stat["Innodb_os_file_reads"], 454)
	assert.EqualValues(t, stat["Innodb_os_file_writes"], 3)
	assert.EqualValues(t, stat["Innodb_os_file_fsyncs"], 3)
	assert.EqualValues(t, stat["Innodb_pending_normal_aio_reads"], 0)
	assert.EqualValues(t, stat["Innodb_pending_normal_aio_writes"], 0)
	assert.EqualValues(t, stat["Innodb_pending_ibuf_aio_reads"], 0)
	assert.EqualValues(t, stat["Innodb_pending_aio_log_ios"], 0)
	assert.EqualValues(t, stat["Innodb_pending_aio_sync_ios"], 0)
	assert.EqualValues(t, stat["Innodb_pending_log_flushes"], 0)
	assert.EqualValues(t, stat["Innodb_pending_buf_pool_flushes"], 0)
	// Insert Buffer and Adaptive Hash Index
	assert.EqualValues(t, stat["Innodb_ibuf_size"], 1)
	assert.EqualValues(t, stat["Innodb_ibuf_free_list"], 0)
	assert.EqualValues(t, stat["Innodb_ibuf_segment_size"], 2)
	assert.EqualValues(t, stat["Innodb_ibuf_merged_inserts"], 0)
	assert.EqualValues(t, stat["Innodb_ibuf_merged_delete_marks"], 0)
	assert.EqualValues(t, stat["Innodb_ibuf_merged_deletes"], 0)
	assert.EqualValues(t, stat["Innodb_hash_index_cells_total"], 276707)
	assert.EqualValues(t, stat["Innodb_hash_index_cells_used"], 0)
	// Log
	assert.EqualValues(t, stat["Innodb_log_writes"], 8)
	assert.EqualValues(t, stat["Innodb_pending_log_writes"], 0)
	assert.EqualValues(t, stat["Innodb_pending_checkpoint_writes"], 0)
	assert.EqualValues(t, stat["Innodb_lsn_current"], 2744156)
	assert.EqualValues(t, stat["Innodb_lsn_flushed"], 2744156)
	assert.EqualValues(t, stat["Innodb_lsn_last_checkpoint"], 2744156)
	// Buffer Pool and Memory
	assert.EqualValues(t, stat["Innodb_mem_total"], 137363456)
	assert.EqualValues(t, stat["Innodb_mem_additional_pool"], 0)
	assert.EqualValues(t, stat["Innodb_mem_adaptive_hash"], 0)
	assert.EqualValues(t, stat["Innodb_mem_page_hash"], 0)
	assert.EqualValues(t, stat["Innodb_mem_dictionary"], 0)
	assert.EqualValues(t, stat["Innodb_mem_file_system"], 0)
	assert.EqualValues(t, stat["Innodb_mem_lock_system"], 0)
	assert.EqualValues(t, stat["Innodb_mem_recovery_system"], 0)
	assert.EqualValues(t, stat["Innodb_mem_thread_hash"], 0)
	assert.EqualValues(t, stat["Innodb_buffer_pool_pages_total"], 8192)
	assert.EqualValues(t, stat["Innodb_buffer_pool_pages_free"], 7749)
	assert.EqualValues(t, stat["Innodb_buffer_pool_pages_data"], 443)
	assert.EqualValues(t, stat["Innodb_buffer_pool_pages_dirty"], 0)
	assert.EqualValues(t, stat["Innodb_buffer_pool_read_ahead"], 0)
	assert.EqualValues(t, stat["Innodb_buffer_pool_read_ahead_evicted"], 0)
	assert.EqualValues(t, stat["Innodb_buffer_pool_read_ahead_rnd"], 0)
	assert.EqualValues(t, stat["Innodb_pages_read"], 443)
	assert.EqualValues(t, stat["Innodb_pages_created"], 0.00)
	assert.EqualValues(t, stat["Innodb_pages_written"], 0.00)
	// Row Operations
	assert.EqualValues(t, stat["Innodb_rows_inserted"], 0)
	assert.EqualValues(t, stat["Innodb_rows_updated"], 0)
	assert.EqualValues(t, stat["Innodb_rows_deleted"], 0)
	assert.EqualValues(t, stat["Innodb_rows_read"], 0)
	assert.EqualValues(t, stat["Innodb_queries_inside"], 0)
	assert.EqualValues(t, stat["Innodb_queries_queued"], 0)
	// etc
	assert.EqualValues(t, stat["Innodb_checkpoint_age"], 0)
}

func TestParseInnodbStatus56(t *testing.T) {
	stat := make(map[string]float64)

	err := parseInnodbStatus(stubInnodbStatus56, &stat)
	assert.NoError(t, err)
	// Innodb Semaphores
	assert.EqualValues(t, stat["Innodb_mutex_spin_waits"], 287)
	assert.EqualValues(t, stat["Innodb_mutex_spin_rounds"], 4777)
	assert.EqualValues(t, stat["Innodb_mutex_os_waits"], 153)
	assert.EqualValues(t, stat["Innodb_semaphore_waits"], 0)
	assert.EqualValues(t, stat["Innodb_mutex_spin_waits"], 287)
	// Innodb Transactions
	assert.EqualValues(t, stat["Innodb_history_list_length"], 1313)
	assert.EqualValues(t, stat["Innodb_current_transactions"], 1)
	assert.EqualValues(t, stat["Innodb_active_transactions"], 0)
	assert.EqualValues(t, stat["Innodb_row_lock_time"], 0)
	assert.EqualValues(t, stat["Innodb_read_views"], 0)
	assert.EqualValues(t, stat["Innodb_tables_in_use"], 0)
	assert.EqualValues(t, stat["Innodb_locked_tables"], 0)
	assert.EqualValues(t, stat["Innodb_lock_structs"], 0)
	assert.EqualValues(t, stat["Innodb_locked_transactions"], 0)
	// File I/O
	assert.EqualValues(t, stat["Innodb_os_file_reads"], 692)
	assert.EqualValues(t, stat["Innodb_os_file_writes"], 6851)
	assert.EqualValues(t, stat["Innodb_os_file_fsyncs"], 3411)
	assert.EqualValues(t, stat["Innodb_pending_normal_aio_reads"], 0)
	assert.EqualValues(t, stat["Innodb_pending_normal_aio_writes"], 0)
	assert.EqualValues(t, stat["Innodb_pending_ibuf_aio_reads"], 0)
	assert.EqualValues(t, stat["Innodb_pending_aio_log_ios"], 0)
	assert.EqualValues(t, stat["Innodb_pending_aio_sync_ios"], 0)
	assert.EqualValues(t, stat["Innodb_pending_log_flushes"], 0)
	assert.EqualValues(t, stat["Innodb_pending_buf_pool_flushes"], 0)
	// Insert Buffer and Adaptive Hash Index
	assert.EqualValues(t, stat["Innodb_ibuf_size"], 1)
	assert.EqualValues(t, stat["Innodb_ibuf_free_list"], 0)
	assert.EqualValues(t, stat["Innodb_ibuf_segment_size"], 2)
	assert.EqualValues(t, stat["Innodb_ibuf_merged_inserts"], 0)
	assert.EqualValues(t, stat["Innodb_ibuf_merged_delete_marks"], 0)
	assert.EqualValues(t, stat["Innodb_ibuf_merged_deletes"], 0)
	assert.EqualValues(t, stat["Innodb_hash_index_cells_total"], 276671)
	assert.EqualValues(t, stat["Innodb_hash_index_cells_used"], 0)
	// Log
	assert.EqualValues(t, stat["Innodb_log_writes"], 1753)
	assert.EqualValues(t, stat["Innodb_pending_log_writes"], 0)
	assert.EqualValues(t, stat["Innodb_pending_checkpoint_writes"], 0)
	assert.EqualValues(t, stat["Innodb_lsn_current"], 21288221)
	assert.EqualValues(t, stat["Innodb_lsn_flushed"], 21288221)
	assert.EqualValues(t, stat["Innodb_lsn_last_checkpoint"], 21288221)
	// Buffer Pool and Memory
	assert.EqualValues(t, stat["Innodb_mem_total"], 137363456)
	assert.EqualValues(t, stat["Innodb_mem_additional_pool"], 0)
	assert.EqualValues(t, stat["Innodb_mem_adaptive_hash"], 0)
	assert.EqualValues(t, stat["Innodb_mem_page_hash"], 0)
	assert.EqualValues(t, stat["Innodb_mem_dictionary"], 0)
	assert.EqualValues(t, stat["Innodb_mem_file_system"], 0)
	assert.EqualValues(t, stat["Innodb_mem_lock_system"], 0)
	assert.EqualValues(t, stat["Innodb_mem_recovery_system"], 0)
	assert.EqualValues(t, stat["Innodb_mem_thread_hash"], 0)
	assert.EqualValues(t, stat["Innodb_buffer_pool_pages_total"], 8191)
	assert.EqualValues(t, stat["Innodb_buffer_pool_pages_free"], 7669)
	assert.EqualValues(t, stat["Innodb_buffer_pool_pages_data"], 518)
	assert.EqualValues(t, stat["Innodb_buffer_pool_pages_dirty"], 0)
	assert.EqualValues(t, stat["Innodb_buffer_pool_read_ahead"], 0)
	assert.EqualValues(t, stat["Innodb_buffer_pool_read_ahead_evicted"], 0)
	assert.EqualValues(t, stat["Innodb_buffer_pool_read_ahead_rnd"], 0)
	assert.EqualValues(t, stat["Innodb_pages_read"], 507)
	assert.EqualValues(t, stat["Innodb_pages_created"], 11)
	assert.EqualValues(t, stat["Innodb_pages_written"], 4589)
	// Row Operations
	assert.EqualValues(t, stat["Innodb_rows_inserted"], 93)
	assert.EqualValues(t, stat["Innodb_rows_updated"], 645)
	assert.EqualValues(t, stat["Innodb_rows_deleted"], 27)
	assert.EqualValues(t, stat["Innodb_rows_read"], 362189)
	assert.EqualValues(t, stat["Innodb_queries_inside"], 0)
	assert.EqualValues(t, stat["Innodb_queries_queued"], 0)
	// etc
	assert.EqualValues(t, stat["Innodb_checkpoint_age"], 0)
}

func TestParseValue(t *testing.T) {
	testCases := []struct {
		rawByte   sql.RawBytes
		value     float64
		boolValue bool
	}{
		{sql.RawBytes("Yes"), 1, true},
		{sql.RawBytes("No"), 0, false},
		{sql.RawBytes("ON"), 1, true},
		{sql.RawBytes("OFF"), 0, false},
		{sql.RawBytes("ABC"), 0, false},
	}
	for _, cases := range testCases {
		if value, ok := parseValue(cases.rawByte); value != cases.value && ok != cases.boolValue {
			t.Errorf("want %d with %t, got %d with %t", int(cases.value), cases.boolValue, int(value), ok)
		}
	}
}
