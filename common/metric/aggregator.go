package metric

// Aggregator XXX
type Aggregator interface {
	AddMetrics(metricType string,
		prefix string,
		fields map[string]interface{},
		tags []string,
		deviceName string,
		t ...int64)

	Add(metricType string, metric Metric)
	Flush()
	Format(metric Metric) interface{}
}
