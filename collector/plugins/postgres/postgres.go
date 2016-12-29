package postgres

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/cloudinsight/cloudinsight-agent/collector"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
	"github.com/cloudinsight/cloudinsight-agent/common/util"
	"github.com/lib/pq"
)

// NewPostgres XXX
func NewPostgres(conf plugin.InitConfig) plugin.Plugin {
	return &Postgres{}
}

// Postgres XXX
type Postgres struct {
	Address       string
	Tags          []string
	Relations     []relationConfig
	CustomMetrics []metricSchema `yaml:"custom_metrics"`

	version          string
	formattedAddress string
}

const (
	// metric types
	gauge     = "gauge"
	rate      = "rate"
	monotonic = "monotoniccount"

	versionQuery     = `SHOW SERVER_VERSION`
	maxCustomResults = 100
)

type tagField struct {
	Name  string
	Alias string
}

type metricSchema struct {
	Descriptors []tagField
	Metrics     map[string]metric.Field
	Query       string
	Relation    bool
}

type relationConfig struct {
	RelationName string `yaml:"relation_name"`
	Schemas      []string
}

var (
	dbMetrics = metricSchema{
		Descriptors: []tagField{
			{
				Name:  "datname",
				Alias: "db",
			},
		},
		Metrics: map[string]metric.Field{},
		Query: `
SELECT datname,
       %s
  FROM pg_stat_database
 WHERE datname not ilike 'template%%'
   AND datname not ilike 'postgres'
   AND datname not ilike 'rdsadmin'
`,
		Relation: false,
	}

	commonMetrics = map[string]metric.Field{
		"numbackends":                                   {"postgresql.connections", gauge},
		"xact_commit":                                   {"postgresql.commits", rate},
		"xact_rollback":                                 {"postgresql.rollbacks", rate},
		"blks_read":                                     {"postgresql.disk_read", rate},
		"blks_hit":                                      {"postgresql.buffer_hit", rate},
		"tup_returned":                                  {"postgresql.rows_returned", rate},
		"tup_fetched":                                   {"postgresql.rows_fetched", rate},
		"tup_inserted":                                  {"postgresql.rows_inserted", rate},
		"tup_updated":                                   {"postgresql.rows_updated", rate},
		"tup_deleted":                                   {"postgresql.rows_deleted", rate},
		"pg_database_size(datname) AS pg_database_size": {"postgresql.database_size", gauge},
	}

	newer92Metrics = map[string]metric.Field{
		"deadlocks":  {"postgresql.deadlocks", rate},
		"temp_bytes": {"postgresql.temp_bytes", rate},
		"temp_files": {"postgresql.temp_files", rate},
	}

	bgwMetrics = metricSchema{
		Descriptors: []tagField{},
		Metrics:     map[string]metric.Field{},
		Query:       "SELECT %s FROM pg_stat_bgwriter",
		Relation:    false,
	}

	commonBGWMetrics = map[string]metric.Field{
		"checkpoints_timed":  {"postgresql.bgwriter.checkpoints_timed", monotonic},
		"checkpoints_req":    {"postgresql.bgwriter.checkpoints_requested", monotonic},
		"buffers_checkpoint": {"postgresql.bgwriter.buffers_checkpoint", monotonic},
		"buffers_clean":      {"postgresql.bgwriter.buffers_clean", monotonic},
		"maxwritten_clean":   {"postgresql.bgwriter.maxwritten_clean", monotonic},
		"buffers_backend":    {"postgresql.bgwriter.buffers_backend", monotonic},
		"buffers_alloc":      {"postgresql.bgwriter.buffers_alloc", monotonic},
	}

	newer91BGWMetrics = map[string]metric.Field{
		"buffers_backend_fsync": {"postgresql.bgwriter.buffers_backend_fsync", monotonic},
	}

	newer92BGWMetrics = map[string]metric.Field{
		"checkpoint_write_time": {"postgresql.bgwriter.write_time", monotonic},
		"checkpoint_sync_time":  {"postgresql.bgwriter.sync_time", monotonic},
	}

	lockMetrics = metricSchema{
		Descriptors: []tagField{
			{
				Name:  "mode",
				Alias: "lock_mode",
			},
			{
				Name:  "relname",
				Alias: "table",
			},
		},
		Metrics: map[string]metric.Field{
			"lock_count": {"postgresql.locks", gauge},
		},
		Query: `
SELECT mode,
       pc.relname,
       count(*) AS %s
  FROM pg_locks l
  JOIN pg_class pc ON (l.relation = pc.oid)
 WHERE l.mode IS NOT NULL
   AND pc.relname NOT LIKE 'pg_%%'
 GROUP BY pc.relname, mode
`,
		Relation: false,
	}

	relMetrics = metricSchema{
		Descriptors: []tagField{
			{
				Name:  "relname",
				Alias: "table",
			},
			{
				Name:  "schemaname",
				Alias: "schema",
			},
		},
		Metrics: map[string]metric.Field{
			"seq_scan":      {"postgresql.seq_scans", rate},
			"seq_tup_read":  {"postgresql.seq_rows_read", rate},
			"idx_scan":      {"postgresql.index_scans", rate},
			"idx_tup_fetch": {"postgresql.index_rows_fetched", rate},
			"n_tup_ins":     {"postgresql.rows_inserted", rate},
			"n_tup_upd":     {"postgresql.rows_updated", rate},
			"n_tup_del":     {"postgresql.rows_deleted", rate},
			"n_tup_hot_upd": {"postgresql.rows_hot_updated", rate},
			"n_live_tup":    {"postgresql.live_rows", gauge},
			"n_dead_tup":    {"postgresql.dead_rows", gauge},
		},
		Query: `
SELECT relname, schemaname, %s
  FROM pg_stat_user_tables
 WHERE relname = ANY($1)
`,
		Relation: true,
	}

	idxMetrics = metricSchema{
		Descriptors: []tagField{
			{
				Name:  "relname",
				Alias: "table",
			},
			{
				Name:  "schemaname",
				Alias: "schema",
			},
			{
				Name:  "indexrelname",
				Alias: "index",
			},
		},
		Metrics: map[string]metric.Field{
			"idx_scan":      {"postgresql.index_scans", rate},
			"idx_tup_read":  {"postgresql.index_rows_read", rate},
			"idx_tup_fetch": {"postgresql.index_rows_fetched", rate},
		},
		Query: `
SELECT relname,
       schemaname,
       indexrelname,
       %s
  FROM pg_stat_user_indexes
 WHERE relname = ANY($1)
`,
		Relation: true,
	}

	sizeMetrics = metricSchema{
		Descriptors: []tagField{
			{
				Name:  "relname",
				Alias: "table",
			},
		},
		Metrics: map[string]metric.Field{
			"pg_table_size(C.oid) as table_size":          {"postgresql.table_size", gauge},
			"pg_indexes_size(C.oid) as index_size":        {"postgresql.index_size", gauge},
			"pg_total_relation_size(C.oid) as total_size": {"postgresql.total_size", gauge},
		},
		Relation: true,
		Query: `
SELECT relname,
       %s
  FROM pg_class C
  LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
 WHERE nspname NOT IN ('pg_catalog', 'information_schema') AND
  nspname !~ '^pg_toast' AND
  relkind IN ('r') AND
  relname = ANY($1)
`,
	}

	countMetrics = metricSchema{
		Descriptors: []tagField{
			{
				Name:  "schemaname",
				Alias: "schema",
			},
		},
		Metrics: map[string]metric.Field{
			"table_count": {"postgresql.table.count", gauge},
		},
		Relation: false,
		Query: `
SELECT schemaname, count(*) AS %s
  FROM pg_stat_user_tables
 GROUP BY schemaname
`,
	}

	replicationMetrics91 = map[string]metric.Field{
		`CASE WHEN pg_last_xlog_receive_location() = pg_last_xlog_replay_location()
		 THEN 0 ELSE GREATEST(0, EXTRACT (EPOCH FROM now() - pg_last_xact_replay_timestamp()))
		 END AS replication_delay`: {"postgresql.replication_delay", gauge},
	}

	replicationMetrics92 = map[string]metric.Field{
		`abs(pg_xlog_location_diff(pg_last_xlog_receive_location(), pg_last_xlog_replay_location()))
		 AS replication_delay_bytes`: {"postgresql.replication_delay_bytes", gauge},
	}

	replicationMetrics = metricSchema{
		Descriptors: []tagField{},
		Metrics:     map[string]metric.Field{},
		Relation:    false,
		Query: `
SELECT %s
 WHERE (SELECT pg_is_in_recovery())
`,
	}

	connectionMetrics = metricSchema{
		Descriptors: []tagField{},
		Metrics: map[string]metric.Field{
			"MAX(setting) AS max_connections":                  {"postgresql.max_connections", gauge},
			"SUM(numbackends)/MAX(setting) AS pct_connections": {"postgresql.percent_usage_connections", gauge},
		},
		Relation: false,
		Query: `
WITH max_con AS (SELECT setting::float FROM pg_settings WHERE name = 'max_connections')
SELECT %s
  FROM pg_stat_database, max_con
`,
	}

	statioMetrics = metricSchema{
		Descriptors: []tagField{
			{
				Name:  "relname",
				Alias: "table",
			},
			{
				Name:  "schemaname",
				Alias: "schema",
			},
		},
		Metrics: map[string]metric.Field{
			"heap_blks_read":  {"postgresql.heap_blocks_read", rate},
			"heap_blks_hit":   {"postgresql.heap_blocks_hit", rate},
			"idx_blks_read":   {"postgresql.index_blocks_read", rate},
			"idx_blks_hit":    {"postgresql.index_blocks_hit", rate},
			"toast_blks_read": {"postgresql.toast_blocks_read", rate},
			"toast_blks_hit":  {"postgresql.toast_blocks_hit", rate},
			"tidx_blks_read":  {"postgresql.toast_index_blocks_read", rate},
			"tidx_blks_hit":   {"postgresql.toast_index_blocks_hit", rate},
		},
		Query: `
SELECT relname,
       schemaname,
       %s
  FROM pg_statio_user_tables
 WHERE relname = ANY($1)
`,
		Relation: true,
	}
)

var localhost = "host=localhost sslmode=disable"

// Check XXX
func (p *Postgres) Check(agg metric.Aggregator) error {
	if p.Address == "" || p.Address == "localhost" {
		p.Address = localhost
	}

	err := p.formatAddress()
	if err != nil {
		return err
	}

	db, err := sql.Open("postgres", p.Address)
	if err != nil {
		log.Errorf("FetchMetrics: %s", err)
		return err
	}
	defer db.Close()

	if p.version == "" {
		p.version, err = p.getVersion(db)
		if err != nil {
			return err
		}
	}
	log.Debugf("Running check against version %s", p.version)

	err = p.collectMetrics(db, agg)
	if err != nil {
		return err
	}

	return nil
}

func (p *Postgres) collectMetrics(db *sql.DB, agg metric.Aggregator) error {
	var fullMetricSchemas []metricSchema
	var err error

	if len(dbMetrics.Metrics) == 0 {
		p.updateDBMetrics()
	}
	fullMetricSchemas = append(fullMetricSchemas, dbMetrics)

	if len(bgwMetrics.Metrics) == 0 {
		p.updateBGWMetrics()
	}
	fullMetricSchemas = append(fullMetricSchemas, bgwMetrics)

	if len(replicationMetrics.Metrics) == 0 {
		p.updateReplMetrics()
	}
	fullMetricSchemas = append(fullMetricSchemas, replicationMetrics)

	fullMetricSchemas = append(fullMetricSchemas, connectionMetrics)
	fullMetricSchemas = append(fullMetricSchemas, lockMetrics)
	fullMetricSchemas = append(fullMetricSchemas, countMetrics)

	if p.Relations != nil {
		fullMetricSchemas = append(fullMetricSchemas, relMetrics)
		fullMetricSchemas = append(fullMetricSchemas, idxMetrics)
		fullMetricSchemas = append(fullMetricSchemas, sizeMetrics)
		fullMetricSchemas = append(fullMetricSchemas, statioMetrics)
	}

	for _, ms := range fullMetricSchemas {
		err = p.collectStats(ms, db, agg, false)
		if err != nil {
			return err
		}
	}

	if p.CustomMetrics != nil {
		err = p.collectCustomMetrics(db, agg)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Postgres) getVersion(db *sql.DB) (string, error) {
	rows, err := db.Query(versionQuery)
	if err != nil {
		log.Errorf("Failed to execute query. %s", versionQuery)
		return "", err
	}
	defer rows.Close()

	var version string
	for rows.Next() {
		if err := rows.Scan(&version); err != nil {
			return "", err
		}
	}

	return version, nil
}

func (p *Postgres) updateDBMetrics() {
	metric.UpdateMap(dbMetrics.Metrics, commonMetrics)
	if p.version >= "9.2.0" {
		metric.UpdateMap(dbMetrics.Metrics, newer92Metrics)
	}
}

func (p *Postgres) updateBGWMetrics() {
	metric.UpdateMap(bgwMetrics.Metrics, commonBGWMetrics)
	if p.version >= "9.1.0" {
		metric.UpdateMap(bgwMetrics.Metrics, newer91BGWMetrics)
	}
	if p.version >= "9.2.0" {
		metric.UpdateMap(bgwMetrics.Metrics, newer92BGWMetrics)
	}
}

func (p *Postgres) updateReplMetrics() {
	if p.version >= "9.1.0" {
		metric.UpdateMap(replicationMetrics.Metrics, replicationMetrics91)
	}
	if p.version >= "9.2.0" {
		metric.UpdateMap(replicationMetrics.Metrics, replicationMetrics92)
	}
}

func (p *Postgres) collectCustomMetrics(db *sql.DB, agg metric.Aggregator) error {
	for _, ms := range p.CustomMetrics {
		for k, v := range ms.Metrics {
			if !util.StringInSlice(v.Type, []string{"gauge", "rate", "monotonic"}) {
				log.Errorf("Metric type %s is not known. Known methods are gauge, rate, monotonic", v.Type)
				delete(ms.Metrics, k)
			}
		}

		err := p.collectStats(ms, db, agg, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Postgres) collectStats(
	ms metricSchema,
	db *sql.DB,
	agg metric.Aggregator,
	isCustom bool,
) error {
	metrics := ms.Metrics
	relation := ms.Relation
	desc := ms.Descriptors

	query := fmt.Sprintf(ms.Query, strings.Join(getMapKeys(metrics), ", "))

	var rows *sql.Rows
	var err error
	if ms.Relation && p.Relations != nil {
		var relnames []string
		for _, relation := range p.Relations {
			relnames = append(relnames, relation.RelationName)
		}

		log.Debugf("Running query: %s with relations: %s", query, relnames)
		rows, err = db.Query(query, pq.Array(relnames))
	} else {
		log.Debugf("Running query: %s", query)
		rows, err = db.Query(query)
	}

	if err != nil {
		log.Errorf("Failed to execute query. %s", query)
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for i := range columns {
		valuePtrs[i] = &values[i]
	}
	asMetrics := getMetricsWithAsCondition(metrics)

	rowsCount := 0
	for rows.Next() {
		rowsCount++
		if isCustom && rowsCount > maxCustomResults {
			log.Warnf("Query: %s returned more than %d results. Truncating", query, maxCustomResults)
			return nil
		}
		// deconstruct array of variables and send to Scan
		err := rows.Scan(valuePtrs...)
		if err != nil {
			log.Warnf("Failed to scan %s", err)
			continue
		}

		var tags []string
		if relation {
			tags = append(p.Tags, "server:"+p.formattedAddress)
		} else {
			tags = p.Tags
		}

		descMap := make(map[string]string, len(desc))
		for i, col := range columns {
			val := values[i]
			if val == nil {
				continue
			}

			switch col {
			default:
				if i < len(desc) {
					tag := desc[i]

					if tag.Name == col {
						var tagV string
						switch v := val.(type) {
						case string:
							tagV = v
						case []uint8:
							tagV = string(val.([]uint8))
						}

						if tagV != "" {
							tags = append(tags, tag.Alias+":"+tagV)
							if util.StringInSlice(tag.Name, []string{"relname", "schemaname"}) {
								descMap[tag.Alias] = tagV
							}
						}
					}
				} else {
					if relation && p.Relations != nil {
						if s, ok := descMap["schema"]; ok {
							var schemaNotMatch bool

							if t, ok := descMap["table"]; ok {
								for _, relation := range p.Relations {
									if relation.RelationName == t && relation.Schemas != nil &&
										!util.StringInSlice(s, relation.Schemas) {
										schemaNotMatch = true
									}
								}
							}

							if schemaNotMatch {
								break
							}
						}
					}

					if field, ok := metrics[col]; ok {
						agg.Add(field.Type, metric.NewMetric(field.Name, val, tags))
					}

					if field, ok := asMetrics[col]; ok {
						agg.Add(field.Type, metric.NewMetric(field.Name, val, tags))
					}
				}
			}
		}
	}

	if reflect.DeepEqual(ms, dbMetrics) {
		agg.Add("gauge", metric.NewMetric("postgresql.db.count", rowsCount, p.Tags))
	}

	return nil
}

var passwordKVMatcher, _ = regexp.Compile("password=\\S+ ?")

func (p *Postgres) formatAddress() error {
	var addr string
	var err error
	if strings.HasPrefix(p.Address, "postgres://") || strings.HasPrefix(p.Address, "postgresql://") {
		addr, err = pq.ParseURL(p.Address)
		if err != nil {
			return err
		}
	} else {
		addr = p.Address
	}
	p.formattedAddress = passwordKVMatcher.ReplaceAllString(addr, "")

	return nil
}

func getMapKeys(metrics map[string]metric.Field) []string {
	keys := make([]string, 0, len(metrics))
	for k := range metrics {
		keys = append(keys, k)
	}
	return keys
}

func getMetricsWithAsCondition(metrics map[string]metric.Field) map[string]metric.Field {
	m := make(map[string]metric.Field)
	for k, v := range metrics {
		if strings.Contains(k, " as ") || strings.Contains(k, " AS ") {
			k = strings.Replace(k, " as ", " AS ", -1)
			key := strings.SplitN(k, " AS ", 2)[1]
			m[key] = v
		}
	}
	return m
}

func init() {
	collector.Add("postgres", NewPostgres)
}
