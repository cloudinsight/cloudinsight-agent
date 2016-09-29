package metric

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/log"
)

// Aggregator XXX
type Aggregator interface {
	AddMetrics(metricType string,
		prefix string,
		fields map[string]interface{},
		tags []string,
		deviceName string,
		t ...int64)

	SubmitPackets(packets []string)
	Add(metricType string, metric Metric)
	Flush()
}

// DefaultExpirySeconds is default to 5 minute
const DefaultExpirySeconds = 5 * 60

// NewAggregator XXX
func NewAggregator(
	metrics chan Metric,
	interval float64,
	hostname string,
	formatter Formatter,
	expiry ...int64,
) Aggregator {
	var expirySeconds int64
	if len(expiry) > 0 {
		expirySeconds = expiry[0]
	} else {
		expirySeconds = DefaultExpirySeconds
	}

	return &aggregator{
		metrics:       metrics,
		context:       make(map[Context]Generator),
		interval:      interval,
		expirySeconds: expirySeconds,
		hostname:      hostname,
		formatter:     formatter,
	}
}

// MetricAggregator XXX
type aggregator struct {
	metrics       chan Metric
	context       map[Context]Generator
	interval      float64
	hostname      string
	formatter     Formatter
	expirySeconds int64
}

// AddMetrics XXX
func (agg *aggregator) AddMetrics(
	metricType string,
	prefix string,
	fields map[string]interface{},
	tags []string,
	deviceName string,
	t ...int64,
) {
	if len(prefix) == 0 || len(fields) == 0 {
		return
	}

	var timestamp int64
	if len(t) > 0 {
		timestamp = t[0]
	} else {
		timestamp = time.Now().Unix()
	}

	for name, value := range fields {
		agg.Add(metricType, Metric{
			Name:       strings.Join([]string{prefix, name}, "."),
			Value:      value,
			Tags:       tags,
			DeviceName: deviceName,
			Timestamp:  timestamp,
		})
	}
}

// SubmitPackets XXX
func (agg *aggregator) SubmitPackets(
	packets []string,
) {
	for _, packet := range packets {
		packet = strings.TrimSpace(packet)
		if packet != "" {
			m, err := parsePacket(packet)
			if err != nil {
				log.Error("Error occurred when parsing packet:", err)
				continue
			}

			agg.Add(m.Type, *m)
		}
	}
}

// Add XXX
func (agg *aggregator) Add(metricType string, m Metric) {
	if m.Hostname == "" {
		m.Hostname = agg.hostname
	}

	ctx := m.Context()
	generator, ok := agg.context[ctx]
	if !ok {
		var err error
		generator, err = NewGenerator(metricType, m, agg.formatter)
		if err != nil {
			log.Errorf("Error adding metric [%v]: %s\n", m, err.Error())
			return
		}
		agg.context[ctx] = generator
	}

	value, err := m.GetCorrectedValue()
	if err != nil {
		log.Error(err)
		return
	}

	generator.Sample(value, m.Timestamp)
}

// Flush XXX
func (agg *aggregator) Flush() {
	timestamp := time.Now().Unix()
	for ctx, generator := range agg.context {
		if generator.IsExpired(agg.expirySeconds) {
			log.Debugf("%s hasn't been submitted in %ds. Expiring.", ctx, agg.expirySeconds)

			delete(agg.context, ctx)
			continue
		}

		metrics := generator.Flush(timestamp, agg.interval)
		for _, m := range metrics {
			agg.metrics <- m
		}
	}
}

// Schema of a statsd packet:
// <name>:<value>|<metric_type>|@<sample_rate>|#<tag1_name>:<tag1_value>,<tag2_name>:<tag2_value>
// For example:
// users.online:1|c|@0.5|#country:china,environment:production
// users.online:1|c|#sometagwithnovalue
func parsePacket(
	packet string,
) (*Metric, error) {
	bits := strings.SplitN(packet, ":", 2)
	if len(bits) != 2 {
		log.Infof("Unable to parse metric: %s\n", packet)
		return nil, fmt.Errorf("Error Parsing statsd packet")
	}

	m := Metric{}
	m.Name = bits[0]
	metadata := bits[1]

	// Validate splitting the bit on "|"
	pipesplit := strings.Split(metadata, "|")
	if len(pipesplit) < 2 {
		log.Infof("Unable to parse metric: %s\n", packet)
		return nil, fmt.Errorf("Error parsing statsd packet")
	}

	value, err := strconv.ParseFloat(pipesplit[0], 64)
	if err != nil {
		return nil, fmt.Errorf("Error parsing value from packet %s: %s\n", packet, err.Error())
	}
	m.Value = value

	// Validate metric type
	switch pipesplit[1] {
	case "c":
		m.Type = "counter"
	case "g":
		m.Type = "bucketgauge"
	case "s":
		m.Type = "set"
	case "ms", "h":
		m.Type = "histogram"
	default:
		log.Infof("Error: Statsd Metric type %s unsupported", pipesplit[1])
		return nil, fmt.Errorf("Error Parsing statsd line")
	}

	for _, segment := range pipesplit {
		if strings.Contains(segment, "@") && len(segment) > 1 {
			samplerate, err := strconv.ParseFloat(segment[1:], 64)
			if err != nil || (samplerate < 0 || samplerate > 1) {
				return nil, fmt.Errorf("Error: parsing sample rate, %s, it must be in format like: "+
					"@0.1, @0.5, etc. Ignoring sample rate for packet: %s\n", err.Error(), packet)
			}

			// sample rate successfully parsed
			m.Samplerate = samplerate
		} else if len(segment) > 0 && segment[0] == '#' {
			m.Tags = strings.Split(segment[1:], ",")
		}
	}

	return &m, nil
}
