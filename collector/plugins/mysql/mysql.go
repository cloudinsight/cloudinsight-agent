package mysql

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/collector"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
	"github.com/go-sql-driver/mysql"
)

// NewMySQL XXX
func NewMySQL(conf plugin.InitConfig) plugin.Plugin {
	return &MySQL{}
}

// MySQL XXX
type MySQL struct {
	Server  string
	Tags    []string
	Options Options

	logBinEnabled            bool
	performanceSchemaEnabled bool
}

// Options XXX
type Options struct {
	Replication             bool
	GaleraCluster           bool `yaml:"galera_cluster"`
	ExtraStatusMetrics      bool `yaml:"extra_status_metrics"`
	ExtraInnodbMetrics      bool `yaml:"extra_innodb_metrics"`
	ExtraPerformanceMetrics bool `yaml:"extra_performance_metrics"`
	SchemaSizeMetrics       bool `yaml:"schema_size_metrics"`
	DisableInnodbMetrics    bool `yaml:"diable_innodb_metrics"`
}

const (
	// metric types
	gauge     = "gauge"
	rate      = "rate"
	count     = "count"
	monotonic = "monotoniccount"

	// metric queries
	globalStatusQuery     = `SHOW /*!50002 GLOBAL */ STATUS`
	globalVariablesQuery  = `SHOW GLOBAL VARIABLES`
	innodbStatusQuery     = `SHOW /*!50000 ENGINE */ INNODB STATUS`
	binaryLogsQuery       = `SHOW BINARY LOGS`
	infoSchemaEngineQuery = `
        SELECT engine
		  FROM information_schema.ENGINES
		 WHERE engine="InnoDB"
		   AND support != "no"
		   AND support != "disabled"
	`
	tableSchemaQuery = `
        SELECT table_schema,
               SUM(data_length+index_length)/1024/1024 AS total_mb
		  FROM information_schema.tables
		 GROUP BY table_schema;
	`
)

var (
	// Vars found in "SHOW STATUS;"
	statusVars = map[string]metric.Field{
		// Command Metrics
		"Slow_queries":       {"mysql.performance.slow_queries", rate},
		"Questions":          {"mysql.performance.questions", rate},
		"Queries":            {"mysql.performance.queries", rate},
		"Com_select":         {"mysql.performance.com_select", rate},
		"Com_insert":         {"mysql.performance.com_insert", rate},
		"Com_update":         {"mysql.performance.com_update", rate},
		"Com_delete":         {"mysql.performance.com_delete", rate},
		"Com_replace":        {"mysql.performance.com_replace", rate},
		"Com_load":           {"mysql.performance.com_load", rate},
		"Com_insert_select":  {"mysql.performance.com_insert_select", rate},
		"Com_update_multi":   {"mysql.performance.com_update_multi", rate},
		"Com_delete_multi":   {"mysql.performance.com_delete_multi", rate},
		"Com_replace_select": {"mysql.performance.com_replace_select", rate},
		// Connection Metrics
		"Connections":          {"mysql.net.connections", rate},
		"Max_used_connections": {"mysql.net.max_connections", gauge},
		"Aborted_clients":      {"mysql.net.aborted_clients", rate},
		"Aborted_connects":     {"mysql.net.aborted_connects", rate},
		// Table Cache Metrics
		"Open_files":  {"mysql.performance.open_files", gauge},
		"Open_tables": {"mysql.performance.open_tables", gauge},
		// Network Metrics
		"Bytes_sent":     {"mysql.performance.bytes_sent", rate},
		"Bytes_received": {"mysql.performance.bytes_received", rate},
		// Query Cache Metrics
		"Qcache_hits":          {"mysql.performance.qcache_hits", rate},
		"Qcache_inserts":       {"mysql.performance.qcache_inserts", rate},
		"Qcache_lowmem_prunes": {"mysql.performance.qcache_lowmem_prunes", rate},
		// Table Lock Metrics
		"Table_locks_waited":      {"mysql.performance.table_locks_waited", gauge},
		"Table_locks_waited_rate": {"mysql.performance.table_locks_waited.rate", rate},
		// Temporary Table Metrics
		"Created_tmp_tables":      {"mysql.performance.created_tmp_tables", rate},
		"Created_tmp_disk_tables": {"mysql.performance.created_tmp_disk_tables", rate},
		"Created_tmp_files":       {"mysql.performance.created_tmp_files", rate},
		// Thread Metrics
		"Threads_connected": {"mysql.performance.threads_connected", gauge},
		"Threads_running":   {"mysql.performance.threads_running", gauge},
		// MyISAM Metrics
		"Key_buffer_bytes_unflushed": {"mysql.myisam.key_buffer_bytes_unflushed", gauge},
		"Key_buffer_bytes_used":      {"mysql.myisam.key_buffer_bytes_used", gauge},
		"Key_read_requests":          {"mysql.myisam.key_read_requests", rate},
		"Key_reads":                  {"mysql.myisam.key_reads", rate},
		"Key_write_requests":         {"mysql.myisam.key_write_requests", rate},
		"Key_writes":                 {"mysql.myisam.key_writes", rate},
	}

	// Possibly from "SHOW GLOBAL VARIABLES;"
	variablesVars = map[string]metric.Field{
		"Key_buffer_size":       {"mysql.myisam.key_buffer_size", gauge},
		"Key_cache_utilization": {"mysql.performance.key_cache_utilization", gauge},
		"max_connections":       {"mysql.net.max_connections_available", gauge},
		"query_cache_size":      {"mysql.performance.qcache_size", gauge},
		"table_open_cache":      {"mysql.performance.table_open_cache", gauge},
		"thread_cache_size":     {"mysql.performance.thread_cache_size", gauge},
	}

	// Vars found in "SHOW /*!50000 ENGINE*/ INNODB STATUS;"
	innodbVars = map[string]metric.Field{
		// InnoDB metrics
		"Innodb_data_reads":                    {"mysql.innodb.data_reads", rate},
		"Innodb_data_writes":                   {"mysql.innodb.data_writes", rate},
		"Innodb_os_log_fsyncs":                 {"mysql.innodb.os_log_fsyncs", rate},
		"Innodb_mutex_spin_waits":              {"mysql.innodb.mutex_spin_waits", rate},
		"Innodb_mutex_spin_rounds":             {"mysql.innodb.mutex_spin_rounds", rate},
		"Innodb_mutex_os_waits":                {"mysql.innodb.mutex_os_waits", rate},
		"Innodb_row_lock_waits":                {"mysql.innodb.row_lock_waits", rate},
		"Innodb_row_lock_time":                 {"mysql.innodb.row_lock_time", rate},
		"Innodb_row_lock_current_waits":        {"mysql.innodb.row_lock_current_waits", gauge},
		"Innodb_current_row_locks":             {"mysql.innodb.current_row_locks", gauge},
		"Innodb_buffer_pool_bytes_dirty":       {"mysql.innodb.buffer_pool_dirty", gauge},
		"Innodb_buffer_pool_bytes_free":        {"mysql.innodb.buffer_pool_free", gauge},
		"Innodb_buffer_pool_bytes_used":        {"mysql.innodb.buffer_pool_used", gauge},
		"Innodb_buffer_pool_bytes_total":       {"mysql.innodb.buffer_pool_total", gauge},
		"Innodb_buffer_pool_read_requests":     {"mysql.innodb.buffer_pool_read_requests", rate},
		"Innodb_buffer_pool_reads":             {"mysql.innodb.buffer_pool_reads", rate},
		"Innodb_buffer_pool_pages_utilization": {"mysql.innodb.buffer_pool_utilization", gauge},
	}

	// Calculated from "SHOW MASTER LOGS;"
	binlogVars = map[string]metric.Field{
		"Binlog_space_usage_bytes": {"mysql.binlog.disk_use", gauge},
	}

	// Additional Vars found in "SHOW STATUS;"
	// Will collect if [FLAG NAME] is True
	optionalStatusVars = map[string]metric.Field{
		"Binlog_cache_disk_use":      {"mysql.binlog.cache_disk_use", gauge},
		"Binlog_cache_use":           {"mysql.binlog.cache_use", gauge},
		"Handler_commit":             {"mysql.performance.handler_commit", rate},
		"Handler_delete":             {"mysql.performance.handler_delete", rate},
		"Handler_prepare":            {"mysql.performance.handler_prepare", rate},
		"Handler_read_first":         {"mysql.performance.handler_read_first", rate},
		"Handler_read_key":           {"mysql.performance.handler_read_key", rate},
		"Handler_read_next":          {"mysql.performance.handler_read_next", rate},
		"Handler_read_prev":          {"mysql.performance.handler_read_prev", rate},
		"Handler_read_rnd":           {"mysql.performance.handler_read_rnd", rate},
		"Handler_read_rnd_next":      {"mysql.performance.handler_read_rnd_next", rate},
		"Handler_rollback":           {"mysql.performance.handler_rollback", rate},
		"Handler_update":             {"mysql.performance.handler_update", rate},
		"Handler_write":              {"mysql.performance.handler_write", rate},
		"Opened_tables":              {"mysql.performance.opened_tables", rate},
		"Qcache_total_blocks":        {"mysql.performance.qcache_total_blocks", gauge},
		"Qcache_free_blocks":         {"mysql.performance.qcache_free_blocks", gauge},
		"Qcache_free_memory":         {"mysql.performance.qcache_free_memory", gauge},
		"Qcache_not_cached":          {"mysql.performance.qcache_not_cached", rate},
		"Qcache_queries_in_cache":    {"mysql.performance.qcache_queries_in_cache", gauge},
		"Select_full_join":           {"mysql.performance.select_full_join", rate},
		"Select_full_range_join":     {"mysql.performance.select_full_range_join", rate},
		"Select_range":               {"mysql.performance.select_range", rate},
		"Select_range_check":         {"mysql.performance.select_range_check", rate},
		"Select_scan":                {"mysql.performance.select_scan", rate},
		"Sort_merge_passes":          {"mysql.performance.sort_merge_passes", rate},
		"Sort_range":                 {"mysql.performance.sort_range", rate},
		"Sort_rows":                  {"mysql.performance.sort_rows", rate},
		"Sort_scan":                  {"mysql.performance.sort_scan", rate},
		"Table_locks_immediate":      {"mysql.performance.table_locks_immediate", gauge},
		"Table_locks_immediate_rate": {"mysql.performance.table_locks_immediate.rate", rate},
		"Threads_cached":             {"mysql.performance.threads_cached", gauge},
		"Threads_created":            {"mysql.performance.threads_created", monotonic},
	}

	// Status Vars added in Mysql 5.6.6
	optionalStatusVars5_6_6 = map[string]metric.Field{
		"Table_open_cache_hits":   {"mysql.performance.table_cache_hits", rate},
		"Table_open_cache_misses": {"mysql.performance.table_cache_misses", rate},
	}

	// Will collect if [extra_innodb_metrics] is True
	optionalInnodbVars = map[string]metric.Field{
		"Innodb_active_transactions":            {"mysql.innodb.active_transactions", gauge},
		"Innodb_buffer_pool_bytes_data":         {"mysql.innodb.buffer_pool_data", gauge},
		"Innodb_buffer_pool_pages_data":         {"mysql.innodb.buffer_pool_pages_data", gauge},
		"Innodb_buffer_pool_pages_dirty":        {"mysql.innodb.buffer_pool_pages_dirty", gauge},
		"Innodb_buffer_pool_pages_flushed":      {"mysql.innodb.buffer_pool_pages_flushed", rate},
		"Innodb_buffer_pool_pages_free":         {"mysql.innodb.buffer_pool_pages_free", gauge},
		"Innodb_buffer_pool_pages_total":        {"mysql.innodb.buffer_pool_pages_total", gauge},
		"Innodb_buffer_pool_read_ahead":         {"mysql.innodb.buffer_pool_read_ahead", rate},
		"Innodb_buffer_pool_read_ahead_evicted": {"mysql.innodb.buffer_pool_read_ahead_evicted", rate},
		"Innodb_buffer_pool_read_ahead_rnd":     {"mysql.innodb.buffer_pool_read_ahead_rnd", gauge},
		"Innodb_buffer_pool_wait_free":          {"mysql.innodb.buffer_pool_wait_free", monotonic},
		"Innodb_buffer_pool_write_requests":     {"mysql.innodb.buffer_pool_write_requests", rate},
		"Innodb_checkpoint_age":                 {"mysql.innodb.checkpoint_age", gauge},
		"Innodb_current_transactions":           {"mysql.innodb.current_transactions", gauge},
		"Innodb_data_fsyncs":                    {"mysql.innodb.data_fsyncs", rate},
		"Innodb_data_pending_fsyncs":            {"mysql.innodb.data_pending_fsyncs", gauge},
		"Innodb_data_pending_reads":             {"mysql.innodb.data_pending_reads", gauge},
		"Innodb_data_pending_writes":            {"mysql.innodb.data_pending_writes", gauge},
		"Innodb_data_read":                      {"mysql.innodb.data_read", rate},
		"Innodb_data_written":                   {"mysql.innodb.data_written", rate},
		"Innodb_dblwr_pages_written":            {"mysql.innodb.dblwr_pages_written", rate},
		"Innodb_dblwr_writes":                   {"mysql.innodb.dblwr_writes", rate},
		"Innodb_hash_index_cells_total":         {"mysql.innodb.hash_index_cells_total", gauge},
		"Innodb_hash_index_cells_used":          {"mysql.innodb.hash_index_cells_used", gauge},
		"Innodb_history_list_length":            {"mysql.innodb.history_list_length", gauge},
		"Innodb_ibuf_free_list":                 {"mysql.innodb.ibuf_free_list", gauge},
		"Innodb_ibuf_merged":                    {"mysql.innodb.ibuf_merged", rate},
		"Innodb_ibuf_merged_delete_marks":       {"mysql.innodb.ibuf_merged_delete_marks", rate},
		"Innodb_ibuf_merged_deletes":            {"mysql.innodb.ibuf_merged_deletes", rate},
		"Innodb_ibuf_merged_inserts":            {"mysql.innodb.ibuf_merged_inserts", rate},
		"Innodb_ibuf_merges":                    {"mysql.innodb.ibuf_merges", rate},
		"Innodb_ibuf_segment_size":              {"mysql.innodb.ibuf_segment_size", gauge},
		"Innodb_ibuf_size":                      {"mysql.innodb.ibuf_size", gauge},
		"Innodb_lock_structs":                   {"mysql.innodb.lock_structs", rate},
		"Innodb_locked_tables":                  {"mysql.innodb.locked_tables", gauge},
		"Innodb_locked_transactions":            {"mysql.innodb.locked_transactions", gauge},
		"Innodb_log_waits":                      {"mysql.innodb.log_waits", rate},
		"Innodb_log_write_requests":             {"mysql.innodb.log_write_requests", rate},
		"Innodb_log_writes":                     {"mysql.innodb.log_writes", rate},
		"Innodb_lsn_current":                    {"mysql.innodb.lsn_current", rate},
		"Innodb_lsn_flushed":                    {"mysql.innodb.lsn_flushed", rate},
		"Innodb_lsn_last_checkpoint":            {"mysql.innodb.lsn_last_checkpoint", rate},
		"Innodb_mem_adaptive_hash":              {"mysql.innodb.mem_adaptive_hash", gauge},
		"Innodb_mem_additional_pool":            {"mysql.innodb.mem_additional_pool", gauge},
		"Innodb_mem_dictionary":                 {"mysql.innodb.mem_dictionary", gauge},
		"Innodb_mem_file_system":                {"mysql.innodb.mem_file_system", gauge},
		"Innodb_mem_lock_system":                {"mysql.innodb.mem_lock_system", gauge},
		"Innodb_mem_page_hash":                  {"mysql.innodb.mem_page_hash", gauge},
		"Innodb_mem_recovery_system":            {"mysql.innodb.mem_recovery_system", gauge},
		"Innodb_mem_thread_hash":                {"mysql.innodb.mem_thread_hash", gauge},
		"Innodb_mem_total":                      {"mysql.innodb.mem_total", gauge},
		"Innodb_os_file_fsyncs":                 {"mysql.innodb.os_file_fsyncs", rate},
		"Innodb_os_file_reads":                  {"mysql.innodb.os_file_reads", rate},
		"Innodb_os_file_writes":                 {"mysql.innodb.os_file_writes", rate},
		"Innodb_os_log_pending_fsyncs":          {"mysql.innodb.os_log_pending_fsyncs", gauge},
		"Innodb_os_log_pending_writes":          {"mysql.innodb.os_log_pending_writes", gauge},
		"Innodb_os_log_written":                 {"mysql.innodb.os_log_written", rate},
		"Innodb_pages_created":                  {"mysql.innodb.pages_created", rate},
		"Innodb_pages_read":                     {"mysql.innodb.pages_read", rate},
		"Innodb_pages_written":                  {"mysql.innodb.pages_written", rate},
		"Innodb_pending_aio_log_ios":            {"mysql.innodb.pending_aio_log_ios", gauge},
		"Innodb_pending_aio_sync_ios":           {"mysql.innodb.pending_aio_sync_ios", gauge},
		"Innodb_pending_buffer_pool_flushes":    {"mysql.innodb.pending_buffer_pool_flushes", gauge},
		"Innodb_pending_checkpoint_writes":      {"mysql.innodb.pending_checkpoint_writes", gauge},
		"Innodb_pending_ibuf_aio_reads":         {"mysql.innodb.pending_ibuf_aio_reads", gauge},
		"Innodb_pending_log_flushes":            {"mysql.innodb.pending_log_flushes", gauge},
		"Innodb_pending_log_writes":             {"mysql.innodb.pending_log_writes", gauge},
		"Innodb_pending_normal_aio_reads":       {"mysql.innodb.pending_normal_aio_reads", gauge},
		"Innodb_pending_normal_aio_writes":      {"mysql.innodb.pending_normal_aio_writes", gauge},
		"Innodb_queries_inside":                 {"mysql.innodb.queries_inside", gauge},
		"Innodb_queries_queued":                 {"mysql.innodb.queries_queued", gauge},
		"Innodb_read_views":                     {"mysql.innodb.read_views", gauge},
		"Innodb_rows_deleted":                   {"mysql.innodb.rows_deleted", rate},
		"Innodb_rows_inserted":                  {"mysql.innodb.rows_inserted", rate},
		"Innodb_rows_read":                      {"mysql.innodb.rows_read", rate},
		"Innodb_rows_updated":                   {"mysql.innodb.rows_updated", rate},
		"Innodb_s_lock_os_waits":                {"mysql.innodb.s_lock_os_waits", rate},
		"Innodb_s_lock_spin_rounds":             {"mysql.innodb.s_lock_spin_rounds", rate},
		"Innodb_s_lock_spin_waits":              {"mysql.innodb.s_lock_spin_waits", rate},
		"Innodb_semaphore_wait_time":            {"mysql.innodb.semaphore_wait_time", gauge},
		"Innodb_semaphore_waits":                {"mysql.innodb.semaphore_waits", gauge},
		"Innodb_tables_in_use":                  {"mysql.innodb.tables_in_use", gauge},
		"Innodb_x_lock_os_waits":                {"mysql.innodb.x_lock_os_waits", rate},
		"Innodb_x_lock_spin_rounds":             {"mysql.innodb.x_lock_spin_rounds", rate},
		"Innodb_x_lock_spin_waits":              {"mysql.innodb.x_lock_spin_waits", rate},
	}

	galeraVars = map[string]metric.Field{
		"wsrep_cluster_size":         {"mysql.galera.wsrep_cluster_size", gauge},
		"wsrep_local_recv_queue_avg": {"mysql.galera.wsrep_local_recv_queue_avg", gauge},
		"wsrep_flow_control_paused":  {"mysql.galera.wsrep_flow_control_paused", gauge},
		"wsrep_cert_deps_distance":   {"mysql.galera.wsrep_cert_deps_distance", gauge},
		"wsrep_local_send_queue_avg": {"mysql.galera.wsrep_local_send_queue_avg", gauge},
	}

	performanceVars = map[string]metric.Field{
		"query_run_time_avg":                 {"mysql.performance.query_run_time.avg", gauge},
		"perf_digest_95th_percentile_avg_us": {"mysql.performance.digest_95th_percentile.avg_us", gauge},
	}

	schemaVars = map[string]metric.Field{
		"information_schema_size": {"mysql.info.schema.size", gauge},
	}

	replicaVars = map[string]metric.Field{
		"Seconds_Behind_Master": {"mysql.replication.seconds_behind_master", gauge},
		"Slaves_connected":      {"mysql.replication.slaves_connected", count},
	}

	syntheticVars = map[string]metric.Field{
		"Qcache_utilization":         {"mysql.performance.qcache.utilization", gauge},
		"Qcache_instant_utilization": {"mysql.performance.qcache.utilization.instant", gauge},
	}
)

// Check XXX
func (m *MySQL) Check(agg metric.Aggregator) error {
	serv, err := m.formatDSN()
	if err != nil {
		return err
	}

	db, err := sql.Open("mysql", serv)
	if err != nil {
		return err
	}

	defer db.Close()

	err = m.collectMetrics(db, agg)
	if err != nil {
		return err
	}

	return nil
}

func (m *MySQL) collectMetrics(db *sql.DB, agg metric.Aggregator) error {
	metrics := statusVars
	fields := make(map[string]float64)
	err := m.collectGlobalStatus(db, fields)
	if err != nil {
		return err
	}

	if !m.Options.DisableInnodbMetrics && m.isInnodbEnabled(db) {
		err = m.collectInnodbStatus(db, fields)
		if err != nil {
			return err
		}

		var innodbPageSize, innodbBufferPoolPagesUsed float64
		if val, ok := fields["Innodb_page_size"]; ok {
			innodbPageSize = val
		}
		if total, ok := fields["Innodb_buffer_pool_pages_total"]; ok {
			if free, ok := fields["Innodb_buffer_pool_pages_free"]; ok {
				innodbBufferPoolPagesUsed = total - free
			}
		}

		if innodbPageSize > 0 {
			if _, ok := fields["Innodb_buffer_pool_bytes_data"]; !ok {
				fields["Innodb_buffer_pool_bytes_data"] = fields["Innodb_buffer_pool_pages_data"] * innodbPageSize
			}
			if _, ok := fields["Innodb_buffer_pool_bytes_dirty"]; !ok {
				fields["Innodb_buffer_pool_bytes_dity"] = fields["Innodb_buffer_pool_pages_dirty"] * innodbPageSize
			}
			if _, ok := fields["Innodb_buffer_pool_bytes_free"]; !ok {
				fields["Innodb_buffer_pool_bytes_free"] = fields["Innodb_buffer_pool_pages_free"] * innodbPageSize
			}
			if _, ok := fields["Innodb_buffer_pool_bytes_total"]; !ok {
				fields["Innodb_buffer_pool_bytes_total"] = fields["Innodb_buffer_pool_pages_total"] * innodbPageSize
			}
			if _, ok := fields["Innodb_buffer_pool_pages_utilization"]; !ok {
				fields["Innodb_buffer_pool_pages_utilization"] = innodbBufferPoolPagesUsed / fields["Innodb_buffer_pool_pages_total"]
			}
			if _, ok := fields["Innodb_buffer_pool_bytes_used"]; !ok {
				fields["Innodb_buffer_pool_bytes_used"] = innodbBufferPoolPagesUsed * innodbPageSize
			}
		}

		if m.Options.ExtraInnodbMetrics {
			log.Debug("Collecting Extra Innodb Metrics")
			metric.UpdateMap(metrics, optionalInnodbVars)
		}
	}

	err = m.collectGlobalVariables(db, fields)
	if err != nil {
		return err
	}
	// Compute key cache utilization metric
	var keyBlocksUnused, keyCacheBlockSize, keyBufferSize float64
	if val, ok := fields["Key_blocks_unused"]; ok {
		keyBlocksUnused = val
	}
	if val, ok := fields["key_cache_block_size"]; ok {
		keyCacheBlockSize = val
	}
	if val, ok := fields["key_buffer_size"]; ok {
		keyBufferSize = val
	}
	fields["Key_buffer_size"] = keyBufferSize

	if keyBufferSize > 0 {

		if val, ok := fields["Key_blocks_used"]; ok {
			fields["Key_buffer_bytes_used"] = val * keyCacheBlockSize
		}

		if val, ok := fields["Key_blocks_not_flushed"]; ok {
			fields["Key_buffer_bytes_unflushed"] = val * keyCacheBlockSize
		}

		fields["Key_cache_utilization"] = 1 - ((keyBlocksUnused * keyCacheBlockSize) / keyBufferSize)
	}

	metric.UpdateMap(metrics, variablesVars)
	metric.UpdateMap(metrics, innodbVars)

	if m.logBinEnabled {
		err = m.collectBinaryLogs(db, fields)
		if err != nil {
			return err
		}
		metric.UpdateMap(metrics, binlogVars)
	}

	if m.Options.ExtraStatusMetrics {
		log.Debug("Collecting Extra Status Metrics")
		metric.UpdateMap(metrics, optionalStatusVars)
		metric.UpdateMap(metrics, optionalStatusVars5_6_6)
	}
	if m.Options.GaleraCluster {
		// already in result-set after 'SHOW STATUS' just add vars to collect
		log.Debug("Collecting Galera Metrics.")
		metric.UpdateMap(metrics, galeraVars)
	}
	if m.Options.ExtraPerformanceMetrics && m.performanceSchemaEnabled {
		log.Debug("Collecting Extra Performance Metrics.")
		// TODO
		metric.UpdateMap(metrics, performanceVars)
	}
	if m.Options.SchemaSizeMetrics {
		log.Debug("Collecting Schema Size Metrics.")
		err = m.collectTableSchema(db, agg)
		if err != nil {
			return err
		}
	}
	if m.Options.Replication {
		log.Debug("Collecting Replication Metrics.")
		// TODO
		metric.UpdateMap(metrics, replicaVars)
	}

	m.submitMetrics(fields, metrics, agg)

	return nil
}

func (m *MySQL) submitMetrics(fields map[string]float64, metrics map[string]metric.Field, agg metric.Aggregator) {
	for key, value := range fields {
		if field, ok := metrics[key]; ok {
			agg.Add(field.Type, metric.NewMetric(field.Name, value, m.Tags))
		}
	}
}

// collectGlobalStatuses can be used to get MySQL status metrics
// the mappings of actual names and names of each status to be exported
// to output is provided on mappings variable
func (m *MySQL) collectGlobalStatus(db *sql.DB, fields map[string]float64) error {
	// run query
	rows, err := db.Query(globalStatusQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var key string
	var val sql.RawBytes

	for rows.Next() {
		if err := rows.Scan(&key, &val); err != nil {
			return err
		}

		// convert numeric values to integer
		value, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			continue
		}
		fields[key] = value
	}

	return nil
}

// collectGlobalVariables can be used to fetch all global variables from
// MySQL environment.
func (m *MySQL) collectGlobalVariables(db *sql.DB, fields map[string]float64) error {
	// run query
	rows, err := db.Query(globalVariablesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var key string
	var val sql.RawBytes

	for rows.Next() {
		if err := rows.Scan(&key, &val); err != nil {
			return err
		}

		// parse value, if it is numeric then save, otherwise ignore
		value, ok := parseValue(val)
		if !ok {
			continue
		}

		switch key {
		case "log_bin":
			if value == 1 {
				m.logBinEnabled = true
			}
		case "performance_schema":
			if value == 1 {
				m.performanceSchemaEnabled = true
			}
		default:
			fields[key] = value
		}
	}

	return nil
}

// There are a number of important InnoDB metrics that are reported in InnoDB status but are not otherwise present as part of
// the STATUS variables in MySQL. Majority of these metrics are reported though as a part of STATUS variables in Percona Server
// and MariaDB. Requires querying user to have PROCESS privileges.
func (m *MySQL) collectInnodbStatus(db *sql.DB, fields map[string]float64) error {
	// run query
	var t, name, stat string
	err := db.QueryRow(innodbStatusQuery).Scan(&t, &name, &stat)
	if err != nil {
		log.Warnf("Privilege error or engine unavailable accessing the INNODB status tables (must grant PROCESS): %s", err)
		return err
	}

	if len(stat) > 0 {
		return parseInnodbStatus(stat, &fields)
	}

	return nil
}

// collectBinaryLogs can be used to collect size and count of all binary files
// binlogs metric requires the MySQL server to turn it on in configuration
func (m *MySQL) collectBinaryLogs(db *sql.DB, fields map[string]float64) error {
	// run query
	rows, err := db.Query(binaryLogsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		size     float64
		fileSize float64
		fileName string
	)

	// iterate over rows and count the size and count of files
	for rows.Next() {
		if err := rows.Scan(&fileName, &fileSize); err != nil {
			return err
		}
		size += fileSize
	}
	fields["Binlog_space_usage_bytes"] = size
	return nil
}

// Fetches the avg query execution time per schema and returns the
// value in microseconds
func (m *MySQL) collectTableSchema(db *sql.DB, agg metric.Aggregator) error {
	// run query
	rows, err := db.Query(tableSchemaQuery)
	if err != nil {
		log.Warnf("Avg exec time performance metrics unavailable at this time: %s", err)
		return err
	}
	defer rows.Close()

	var tableSchema string
	var size float64

	for rows.Next() {
		if err := rows.Scan(&tableSchema, &size); err != nil {
			return err
		}

		tags := []string{"schema:" + tableSchema}
		if field, ok := schemaVars["information_schema_size"]; ok {
			agg.Add(field.Type, metric.NewMetric(field.Name, size, tags))
		}
	}

	return nil
}

func (m *MySQL) isInnodbEnabled(db *sql.DB) bool {
	// run query
	var engine string
	err := db.QueryRow(infoSchemaEngineQuery).Scan(&engine)
	if err != nil {
		log.Warnf("Possibly innodb stats unavailable - error querying engines table: %s", err)
		return false
	}

	return true
}

func (m *MySQL) formatDSN() (string, error) {
	conf, err := mysql.ParseDSN(m.Server)
	if err != nil {
		return "", err
	}

	if conf.Timeout == 0 {
		conf.Timeout = time.Second * 5
	}

	return conf.FormatDSN(), nil
}

func parseInnodbStatus(str string, p *map[string]float64) error {
	isTransaction := false
	prevLine := ""

	for _, line := range strings.Split(str, "\n") {
		record := strings.Fields(line)

		// Innodb Semaphores
		if strings.Index(line, "Mutex spin waits") == 0 {
			// Mutex spin waits 79626940, rounds 157459864, OS waits 698719
			// Mutex spin waits 0, rounds 247280272495, OS waits 316513438
			increaseMap(p, "Innodb_mutex_spin_waits", record[3])
			increaseMap(p, "Innodb_mutex_spin_rounds", record[5])
			increaseMap(p, "Innodb_mutex_os_waits", record[8])
			continue
		}
		if strings.Index(line, "RW-shared spins") == 0 && strings.Index(line, ";") > 0 {
			// RW-shared spins 3859028, OS waits 2100750; RW-excl spins
			// 4641946, OS waits 1530310
			increaseMap(p, "Innodb_mutex_os_waits", record[2])
			increaseMap(p, "Innodb_s_lock_os_waits", record[5])
			increaseMap(p, "Innodb_x_lock_spin_waits", record[8])
			increaseMap(p, "Innodb_x_lock_os_waits", record[11])
			continue
		}
		if strings.Index(line, "RW-shared spins") == 0 && strings.Index(line, "; RW-excl spins") < 0 {
			// Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
			// RW-shared spins 604733, rounds 8107431, OS waits 241268
			increaseMap(p, "Innodb_s_lock_spin_waits", record[2])
			increaseMap(p, "Innodb_s_lock_spin_rounds", record[4])
			increaseMap(p, "Innodb_s_lock_os_waits", record[7])
			continue
		}
		if strings.Index(line, "RW-excl spins") == 0 {
			// Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
			// RW-excl spins 604733, rounds 8107431, OS waits 241268
			increaseMap(p, "Innodb_x_lock_spin_waits", record[2])
			increaseMap(p, "Innodb_x_lock_spin_rounds", record[4])
			increaseMap(p, "Innodb_x_lock_os_waits", record[7])
			continue
		}
		if strings.Index(line, "seconds the semaphore:") > 0 {
			// --Thread 907205 has waited at handler/ha_innodb.cc line 7156 for 1.00 seconds the semaphore:
			increaseMap(p, "Innodb_semaphore_waits", "1")
			wait := atof(record[9])
			wait = wait * 1000
			increaseMap(p, "Innodb_mutex_spin_waits", fmt.Sprintf("%.f", wait))
			continue
		}

		// Innodb Transactions
		if strings.Index(line, "Trx id counter") == 0 {
			// The beginning of the TRANSACTIONS section: start counting
			// transactions
			// Trx id counter 0 1170664159
			// Trx id counter 861B144C
			isTransaction = true
			continue
		}
		if strings.Index(line, "History list length") == 0 {
			// History list length 132
			increaseMap(p, "Innodb_history_list_length", record[3])
			continue
		}
		if isTransaction && strings.Index(line, "---TRANSACTION") == 0 {
			// ---TRANSACTION 0, not started, process no 13510, OS thread id 1170446656
			increaseMap(p, "Innodb_current_transactions", "1")
			if strings.Index(line, "ACTIVE") > 0 {
				increaseMap(p, "Innodb_active_transactions", "1")
			}
			continue
		}
		if isTransaction && strings.Index(line, "------- TRX HAS BEEN") == 0 {
			// ------- TRX HAS BEEN WAITING 32 SEC FOR THIS LOCK TO BE GRANTED:
			increaseMap(p, "Innodb_row_lock_time", "1")
			continue
		}
		if strings.Index(line, "read views open inside InnoDB") > 0 {
			// 1 read views open inside InnoDB
			(*p)["Innodb_read_views"] = atof(record[0])
			continue
		}
		if strings.Index(line, "mysql tables in use") == 0 {
			// mysql tables in use 2, locked 2
			increaseMap(p, "Innodb_tables_in_use", record[4])
			increaseMap(p, "Innodb_locked_tables", record[6])
			continue
		}
		if isTransaction && strings.Index(line, "lock struct(s)") == 0 {
			// 23 lock struct(s), heap size 3024, undo log entries 27
			// LOCK WAIT 12 lock struct(s), heap size 3024, undo log entries 5
			// LOCK WAIT 2 lock struct(s), heap size 368
			if strings.Index(line, "LOCK WAIT") > 0 {
				increaseMap(p, "Innodb_lock_structs", record[2])
				increaseMap(p, "Innodb_locked_transactions", "1")
			} else if strings.Index(line, "ROLLING BACK") > 0 {
				// ROLLING BACK 127539 lock struct(s), heap size 15201832,
				// 4411492 row lock(s), undo log entries 1042488
				increaseMap(p, "Innodb_lock_structs", record[0])
			} else {
				increaseMap(p, "Innodb_lock_structs", record[0])
			}
			continue
		}

		// File I/O
		if strings.Index(line, " OS file reads, ") > 0 {
			// 8782182 OS file reads, 15635445 OS file writes, 947800 OS
			// fsyncs
			(*p)["Innodb_os_file_reads"] = atof(record[0])
			(*p)["Innodb_os_file_writes"] = atof(record[4])
			(*p)["Innodb_os_file_fsyncs"] = atof(record[8])
			continue
		}
		if strings.Index(line, "Pending normal aio reads:") == 0 {
			// Pending normal aio reads: 0, aio writes: 0,
			// or Pending normal aio reads: [0, 0, 0, 0] , aio writes: [0, 0, 0, 0] ,
			// or Pending normal aio reads: 0 [0, 0, 0, 0] , aio writes: 0 [0, 0, 0, 0] ,
			if len(record) == 16 {
				(*p)["Innodb_pending_normal_aio_reads"] = atof(record[4]) + atof(record[5]) + atof(record[6]) + atof(record[7])
				(*p)["Innodb_pending_normal_aio_writes"] = atof(record[11]) + atof(record[12]) + atof(record[13]) + atof(record[14])
			} else if len(record) == 18 {
				(*p)["Innodb_pending_normal_aio_reads"] = atof(record[4])
				(*p)["Innodb_pending_normal_aio_writes"] = atof(record[12])
			} else {
				(*p)["Innodb_pending_normal_aio_reads"] = atof(record[4])
				(*p)["Innodb_pending_normal_aio_writes"] = atof(record[7])
			}
			continue
		}
		if strings.Index(line, " ibuf aio reads") == 0 {
			// ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
			// or ibuf aio reads:, log i/o's:, sync i/o's:
			if len(record) == 10 {
				(*p)["Innodb_pending_ibuf_aio_reads"] = atof(record[3])
				(*p)["Innodb_pending_aio_log_ios"] = atof(record[6])
				(*p)["Innodb_pending_aio_sync_ios"] = atof(record[9])
			} else if len(record) == 7 {
				(*p)["Innodb_pending_ibuf_aio_reads"] = 0
				(*p)["Innodb_pending_aio_log_ios"] = 0
				(*p)["Innodb_pending_aio_sync_ios"] = 0
			}
			continue
		}
		if strings.Index(line, "Pending flushes (fsync)") == 0 {
			// Pending flushes (fsync) log: 0; buffer pool: 0
			(*p)["Innodb_pending_log_flushes"] = atof(record[4])
			(*p)["Innodb_pending_buffer_pool_flushes"] = atof(record[7])
			continue
		}

		// Insert Buffer and Adaptive Hash Index
		if strings.Index(line, "Ibuf for space 0: size ") == 0 {
			// Older InnoDB code seemed to be ready for an ibuf per tablespace.  It
			// had two lines in the output.  Newer has just one line, see below.
			// Ibuf for space 0: size 1, free list len 887, seg size 889, is not empty
			// Ibuf for space 0: size 1, free list len 887, seg size 889,
			(*p)["Innodb_ibuf_size"] = atof(record[5])
			(*p)["Innodb_ibuf_free_list"] = atof(record[9])
			(*p)["Innodb_ibuf_segment_size"] = atof(record[12])
			continue
		}
		if strings.Index(line, "Ibuf: size ") == 0 {
			// Ibuf: size 1, free list len 4634, seg size 4636,
			(*p)["Innodb_ibuf_size"] = atof(record[2])
			(*p)["Innodb_ibuf_free_list"] = atof(record[6])
			(*p)["Innodb_ibuf_segment_size"] = atof(record[9])
			if strings.Index(line, "merges") > 0 {
				(*p)["Innodb_ibuf_merges"] = atof(record[10])
			}
			continue
		}
		if strings.Index(line, ", delete mark ") > 0 && strings.Index(prevLine, "merged operations:") == 0 {
			// Output of show engine innodb status has changed in 5.5
			// merged operations:
			// insert 593983, delete mark 387006, delete 73092
			v1 := atof(record[1])
			v2 := atof(record[4])
			v3 := atof(record[6])
			(*p)["Innodb_ibuf_merged_inserts"] = v1
			(*p)["Innodb_ibuf_merged_delete_marks"] = v2
			(*p)["Innodb_ibuf_merged_deletes"] = v3
			(*p)["Innodb_ibuf_merged"] = v1 + v2 + v3
			continue
		}
		if strings.Index(line, " merged recs, ") > 0 {
			// 19817685 inserts, 19817684 merged recs, 3552620 merges
			(*p)["Innodb_ibuf_merged_inserts"] = atof(record[0])
			(*p)["Innodb_ibuf_merged"] = atof(record[2])
			(*p)["Innodb_ibuf_merges"] = atof(record[5])
			continue
		}
		if strings.Index(line, "Hash table size ") == 0 {
			// In some versions of InnoDB, the used cells is omitted.
			// Hash table size 4425293, used cells 4229064, ....
			// Hash table size 57374437, node heap has 72964 buffer(s) <--
			// no used cells
			(*p)["Innodb_hash_index_cells_total"] = atof(record[3])
			if strings.Index(line, "used cells") > 0 {
				(*p)["Innodb_hash_index_cells_used"] = atof(record[6])
			} else {
				(*p)["Innodb_hash_index_cells_used"] = 0
			}
			continue
		}

		// Log
		if strings.Index(line, " log i/o's done, ") > 0 {
			// 3430041 log i/o's done, 17.44 log i/o's/second
			// 520835887 log i/o's done, 17.28 log i/o's/second, 518724686
			// syncs, 2980893 checkpoints
			(*p)["Innodb_log_writes"] = atof(record[0])
			continue
		}
		if strings.Index(line, " pending log writes, ") > 0 {
			// 0 pending log writes, 0 pending chkp writes
			(*p)["Innodb_pending_log_writes"] = atof(record[0])
			(*p)["Innodb_pending_checkpoint_writes"] = atof(record[4])
			continue
		}
		if strings.Index(line, "Log sequence number") == 0 {
			// This number is NOT printed in hex in InnoDB plugin.
			// Log sequence number 272588624
			val := atof(record[3])
			if len(record) >= 5 {
				val = float64(makeBigint(record[3], record[4]))
			}
			(*p)["Innodb_lsn_current"] = val
			continue
		}
		if strings.Index(line, "Log flushed up to") == 0 {
			// This number is NOT printed in hex in InnoDB plugin.
			// Log flushed up to   272588624
			val := atof(record[4])
			if len(record) >= 6 {
				val = float64(makeBigint(record[4], record[5]))
			}
			(*p)["Innodb_lsn_flushed"] = val
			continue
		}
		if strings.Index(line, "Last checkpoint at") == 0 {
			// Last checkpoint at  272588624
			val := atof(record[3])
			if len(record) >= 5 {
				val = float64(makeBigint(record[3], record[4]))
			}
			(*p)["Innodb_lsn_last_checkpoint"] = val
			continue
		}

		// Buffer Pool and Memory
		// 5.6 or before
		if strings.Index(line, "Total memory allocated") == 0 && strings.Index(line, "in additional pool allocated") > 0 {
			// Total memory allocated 29642194944; in additional pool allocated 0
			// Total memory allocated by read views 96
			(*p)["Innodb_mem_total"] = atof(record[3])
			(*p)["Innodb_mem_additional_pool"] = atof(record[8])
			continue
		}
		if strings.Index(line, "Adaptive hash index ") == 0 {
			// Adaptive hash index 1538240664     (186998824 + 1351241840)
			v := atof(record[3])
			setIfEmpty(p, "Innodb_mem_adaptive_hash", v)
			continue
		}
		if strings.Index(line, "Page hash           ") == 0 {
			// Page hash           11688584
			v := atof(record[2])
			setIfEmpty(p, "Innodb_mem_page_hash", v)
			continue
		}
		if strings.Index(line, "Dictionary cache    ") == 0 {
			// Dictionary cache    145525560      (140250984 + 5274576)
			v := atof(record[2])
			setIfEmpty(p, "Innodb_mem_dictionary", v)
			continue
		}
		if strings.Index(line, "File system         ") == 0 {
			// File system         313848         (82672 + 231176)
			v := atof(record[2])
			setIfEmpty(p, "Innodb_mem_file_system", v)
			continue
		}
		if strings.Index(line, "Lock system         ") == 0 {
			// Lock system         29232616       (29219368 + 13248)
			v := atof(record[2])
			setIfEmpty(p, "Innodb_mem_lock_system", v)
			continue
		}
		if strings.Index(line, "Recovery system     ") == 0 {
			// Recovery system     0      (0 + 0)
			v := atof(record[2])
			setIfEmpty(p, "Innodb_mem_recovery_system", v)
			continue
		}
		if strings.Index(line, "Threads             ") == 0 {
			// Threads             409336         (406936 + 2400)
			v := atof(record[1])
			setIfEmpty(p, "Innodb_mem_thread_hash", v)
			continue
		}
		if strings.Index(line, "Buffer pool size ") == 0 {
			// The " " after size is necessary to avoid matching the wrong line:
			// Buffer pool size        1769471
			// Buffer pool size, bytes 28991012864
			v := atof(record[3])
			setIfEmpty(p, "Innodb_buffer_pool_pages_total", v)
			continue
		}
		if strings.Index(line, "Free buffers") == 0 {
			// Free buffers            0
			v := atof(record[2])
			setIfEmpty(p, "Innodb_buffer_pool_pages_free", v)
			continue
		}
		if strings.Index(line, "Database pages") == 0 {
			// Database pages          1696503
			v := atof(record[2])
			setIfEmpty(p, "Innodb_buffer_pool_pages_data", v)
			continue
		}
		if strings.Index(line, "Modified db pages") == 0 {
			// Modified db pages       160602
			v := atof(record[3])
			setIfEmpty(p, "Innodb_buffer_pool_pages_dirty", v)
			continue
		}
		if strings.Index(line, "Pages read ahead") == 0 {
			v := atof(record[3])
			setIfEmpty(p, "Innodb_buffer_pool_read_ahead", v)
			v = atof(record[7])
			setIfEmpty(p, "Innodb_buffer_pool_read_ahead_evicted", v)
			v = atof(record[11])
			setIfEmpty(p, "Innodb_buffer_pool_read_ahead_rnd", v)
			continue
		}
		if strings.Index(line, "Pages read") == 0 {
			// Pages read 15240822, created 1770238, written 21705836
			v := atof(record[2])
			setIfEmpty(p, "Innodb_pages_read", v)
			v = atof(record[4])
			setIfEmpty(p, "Innodb_pages_created", v)
			v = atof(record[6])
			setIfEmpty(p, "Innodb_pages_written", v)
			continue
		}

		// Row Operations
		if strings.Index(line, "Number of rows inserted") == 0 {
			// Number of rows inserted 50678311, updated 66425915, deleted
			// 20605903, read 454561562
			(*p)["Innodb_rows_inserted"] = atof(record[4])
			(*p)["Innodb_rows_updated"] = atof(record[6])
			(*p)["Innodb_rows_deleted"] = atof(record[8])
			(*p)["Innodb_rows_read"] = atof(record[10])
			continue
		}
		if strings.Index(line, " queries inside InnoDB, ") > 0 {
			// 0 queries inside InnoDB, 0 queries in queue
			(*p)["Innodb_queries_inside"] = atof(record[0])
			(*p)["Innodb_queries_queued"] = atof(record[4])
			continue
		}

		// for next loop
		prevLine = line
	}

	// We need to calculate this metric separately
	(*p)["Innodb_checkpoint_age"] = (*p)["Innodb_lsn_current"] - (*p)["Innodb_lsn_last_checkpoint"]

	return nil
}

// parseValue can be used to convert values such as "ON","OFF","Yes","No" to 0,1
func parseValue(value sql.RawBytes) (float64, bool) {
	val := strings.ToLower(string(value))
	if val == "yes" || val == "on" {
		return 1, true
	}

	if val == "no" || val == "off" {
		return 0, true
	}
	n, err := strconv.ParseFloat(val, 64)
	return n, err == nil
}

func setIfEmpty(p *map[string]float64, key string, val float64) {
	_, ok := (*p)[key]
	if !ok {
		(*p)[key] = val
	}
}

func atof(str string) float64 {
	str = strings.Replace(str, ",", "", -1)
	str = strings.Replace(str, ";", "", -1)
	str = strings.Replace(str, "/s", "", -1)
	str = strings.Trim(str, " ")
	val, err := strconv.ParseFloat(str, 64)
	if err != nil {
		log.Debugf("Error occurred when parse %s to float. %s", str, err)
		val = 0
	}
	return val
}

func increaseMap(p *map[string]float64, key string, src string) {
	val := atof(src)
	_, exists := (*p)[key]
	if !exists {
		(*p)[key] = val
		return
	}
	(*p)[key] = (*p)[key] + val
}

func makeBigint(hi string, lo string) int64 {
	if lo == "" {
		val, _ := strconv.ParseInt(hi, 16, 64)
		return val
	}

	var hiVal int64
	var loVal int64
	if hi != "" {
		hiVal, _ = strconv.ParseInt(hi, 10, 64)
	}
	if lo != "" {
		loVal, _ = strconv.ParseInt(lo, 10, 64)
	}

	val := hiVal * loVal

	return val
}

func init() {
	collector.Add("mysql", NewMySQL)
}
