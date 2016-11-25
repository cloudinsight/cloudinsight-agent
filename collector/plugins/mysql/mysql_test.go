package mysql

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseInnodbStatus55(t *testing.T) {

	stub := `=====================================
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
	stat := make(map[string]float64)

	err := parseInnodbStatus(stub, &stat)
	assert.NoError(t, err)
	// Innodb Semaphores
	assert.EqualValues(t, stat["Innodb_mutex_spin_waits"], 2)
	assert.EqualValues(t, stat["Innodb_mutex_spin_rounds"], 31)
	assert.EqualValues(t, stat["Innodb_mutex_os_waits"], 1)
	assert.EqualValues(t, stat["Innodb_semaphore_waits"], 0)  // empty
	assert.EqualValues(t, stat["Innodb_mutex_spin_waits"], 2) // empty
	// Innodb Transactions
	assert.EqualValues(t, stat["Innodb_history_list_length"], 324)
	assert.EqualValues(t, stat["Innodb_current_transactions"], 1)
	assert.EqualValues(t, stat["Innodb_active_transactions"], 0)
	assert.EqualValues(t, stat["Innodb_row_lock_time"], 0) // empty
	assert.EqualValues(t, stat["Innodb_read_views"], 1)
	assert.EqualValues(t, stat["Innodb_tables_in_use"], 0)       // empty
	assert.EqualValues(t, stat["Innodb_locked_tables"], 0)       // empty
	assert.EqualValues(t, stat["Innodb_lock_structs"], 0)        // empty
	assert.EqualValues(t, stat["Innodb_locked_transactions"], 0) // empty
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
	assert.EqualValues(t, stat["Innodb_mem_adaptive_hash"], 0)   // empty
	assert.EqualValues(t, stat["Innodb_mem_page_hash"], 0)       // empty
	assert.EqualValues(t, stat["Innodb_mem_dictionary"], 0)      // empty
	assert.EqualValues(t, stat["Innodb_mem_file_system"], 0)     // empty
	assert.EqualValues(t, stat["Innodb_mem_lock_system"], 0)     // empty
	assert.EqualValues(t, stat["Innodb_mem_recovery_system"], 0) // empty
	assert.EqualValues(t, stat["Innodb_mem_thread_hash"], 0)     // empty
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

	stub := `=====================================
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
	stat := make(map[string]float64)

	err := parseInnodbStatus(stub, &stat)
	assert.NoError(t, err)
	// Innodb Semaphores
	assert.EqualValues(t, stat["Innodb_mutex_spin_waits"], 287)
	assert.EqualValues(t, stat["Innodb_mutex_spin_rounds"], 4777)
	assert.EqualValues(t, stat["Innodb_mutex_os_waits"], 153)
	assert.EqualValues(t, stat["Innodb_semaphore_waits"], 0)    // empty
	assert.EqualValues(t, stat["Innodb_mutex_spin_waits"], 287) // empty
	// Innodb Transactions
	assert.EqualValues(t, stat["Innodb_history_list_length"], 1313)
	assert.EqualValues(t, stat["Innodb_current_transactions"], 1)
	assert.EqualValues(t, stat["Innodb_active_transactions"], 0)
	assert.EqualValues(t, stat["Innodb_row_lock_time"], 0) // empty
	assert.EqualValues(t, stat["Innodb_read_views"], 0)
	assert.EqualValues(t, stat["Innodb_tables_in_use"], 0)       // empty
	assert.EqualValues(t, stat["Innodb_locked_tables"], 0)       // empty
	assert.EqualValues(t, stat["Innodb_lock_structs"], 0)        // empty
	assert.EqualValues(t, stat["Innodb_locked_transactions"], 0) // empty
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
	assert.EqualValues(t, stat["Innodb_mem_adaptive_hash"], 0)   // empty
	assert.EqualValues(t, stat["Innodb_mem_page_hash"], 0)       // empty
	assert.EqualValues(t, stat["Innodb_mem_dictionary"], 0)      // empty
	assert.EqualValues(t, stat["Innodb_mem_file_system"], 0)     // empty
	assert.EqualValues(t, stat["Innodb_mem_lock_system"], 0)     // empty
	assert.EqualValues(t, stat["Innodb_mem_recovery_system"], 0) // empty
	assert.EqualValues(t, stat["Innodb_mem_thread_hash"], 0)     // empty
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
