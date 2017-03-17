package metric

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/util"
)

// Aggregator XXX
type Aggregator interface {
	AddMetrics(metricType string,
		prefix string,
		fields map[string]interface{},
		tags []string,
		deviceName string,
		t ...int64)

	SubmitPackets(packet string)
	Add(metricType string, m Metric)
	Flush()
}

const (
	// DefaultRecentPointThreshold is default to 1 hour
	DefaultRecentPointThreshold = 3600

	// DefaultExpirySeconds is default to 5 minutes
	DefaultExpirySeconds = 5 * 60
)

// NewAggregator creates a new instance of aggregator.
func NewAggregator(
	metrics chan Metric,
	interval float64,
	hostname string,
	formatter Formatter,
	histogramAggregates []string,
	histogramPercentiles []float64,
	recentPointThreshold int64,
	expiry ...int64,
) Aggregator {
	if recentPointThreshold == 0 {
		recentPointThreshold = DefaultRecentPointThreshold
	}

	var expirySeconds int64
	if len(expiry) > 0 {
		expirySeconds = expiry[0]
	} else {
		expirySeconds = DefaultExpirySeconds
	}

	return &aggregator{
		metrics:              metrics,
		context:              make(map[Context]Generator),
		interval:             interval,
		hostname:             hostname,
		formatter:            formatter,
		histogramAggregates:  histogramAggregates,
		histogramPercentiles: histogramPercentiles,
		recentPointThreshold: recentPointThreshold,
		expirySeconds:        expirySeconds,
	}
}

type aggregator struct {
	sync.Mutex

	metrics              chan Metric
	context              map[Context]Generator
	interval             float64
	hostname             string
	formatter            Formatter
	histogramAggregates  []string
	histogramPercentiles []float64
	recentPointThreshold int64
	discardedOldPoints   int64
	expirySeconds        int64
}

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

func (agg *aggregator) SubmitPackets(
	packet string,
) {
	packets := strings.Split(packet, "\n")
	for _, packet := range packets {
		packet = strings.TrimSpace(packet)
		if packet != "" {
			metrics, err := parsePacket(packet)
			if err != nil {
				log.Error("Error occurred when parsing packet:", err)
				continue
			}

			for _, m := range metrics {
				agg.Add(m.Type, m)
			}
		}
	}
}

func (agg *aggregator) Add(metricType string, m Metric) {
	if m.Hostname == "" {
		m.Hostname = agg.hostname
	}

	timestamp := time.Now().Unix()
	if m.Timestamp > 0 && timestamp-m.Timestamp > agg.recentPointThreshold {
		log.Debugf("Discarding %s - ts = %d , current ts = %d ", m.Name, m.Timestamp, timestamp)
		agg.discardedOldPoints++
		return
	}

	agg.Lock()
	defer agg.Unlock()
	ctx := m.context()
	generator, ok := agg.context[ctx]
	if !ok {
		var err error
		generator, err = NewGenerator(metricType, m, agg.formatter, agg.histogramAggregates, agg.histogramPercentiles)
		if err != nil {
			log.Errorf("Error adding metric [%v]: %s", m, err.Error())
			return
		}
		agg.context[ctx] = generator
	}

	value, err := m.getCorrectedValue()
	if err != nil {
		log.Error(err)
		return
	}

	generator.Sample(value, m.Timestamp)
}

func (agg *aggregator) Flush() {
	timestamp := time.Now().Unix()
	for ctx, generator := range agg.context {
		if generator.IsExpired(timestamp, agg.expirySeconds) {
			log.Debugf("%v hasn't been submitted in %ds. Expiring.", ctx, agg.expirySeconds)

			delete(agg.context, ctx)
			continue
		}

		metrics := generator.Flush(timestamp, agg.interval)
		for _, m := range metrics {
			agg.metrics <- m
		}
	}

	// Log a warning regarding metrics with old timestamps being submitted
	if agg.discardedOldPoints > 0 {
		log.Warnf("%d points were discarded as a result of having an old timestamp", agg.discardedOldPoints)
		agg.discardedOldPoints = 0
	}
}

// Schema of a statsd packet:
// <name>:<value>|<metric_type>|@<sample_rate>|#<tag1_name>:<tag1_value>,<tag2_name>:<tag2_value>
// For example:
// users.online:1|c|@0.5|#country:china,environment:production
// users.online:1|c|#sometagwithnovalue
func parsePacket(
	packet string,
) ([]Metric, error) {
	bits := strings.SplitN(packet, ":", 2)
	if len(bits) != 2 {
		log.Infof("Error: splitting ':', Unable to parse metric: %s", packet)
		return nil, fmt.Errorf("Error Parsing statsd packet")
	}

	name := bits[0]
	metadata := bits[1]

	data := []string{}
	var partialDatum string
	brokensplit := strings.Split(metadata, ":")
	// We need to fix the tag groups that got broken by the : split
	for _, token := range brokensplit {
		if partialDatum == "" {
			partialDatum = token
		} else if !strings.Contains(token, "|") || (strings.Contains(token, "@") && len(strings.Split(token, "|")) == 2) {
			partialDatum += ":" + token
		} else {
			data = append(data, partialDatum)
			partialDatum = token
		}
	}
	data = append(data, partialDatum)

	metrics := []Metric{}
	for _, datum := range data {
		m := Metric{}
		m.Name = name
		// Validate splitting the bit on "|"
		pipesplit := strings.Split(datum, "|")
		if len(pipesplit) < 2 {
			log.Infof("Error: splitting '|', Unable to parse metric: %s", packet)
			return nil, fmt.Errorf("Error parsing statsd packet")
		}

		// Set allows value of strings.
		if pipesplit[1] != "s" {
			value, err := strconv.ParseFloat(pipesplit[0], 64)
			if err != nil {
				return nil, fmt.Errorf("Error parsing value from packet %s: %s", packet, err.Error())
			}
			m.Value = value
		}

		// Validate metric type
		switch pipesplit[1] {
		case "c":
			m.Type = "counter"
		case "g":
			m.Type = "bucketgauge"
		case "s":
			m.Type = "set"
			m.Value = util.Hash(pipesplit[0])
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
						"@0.1, @0.5, etc. Ignoring sample rate for packet: %s", err.Error(), packet)
				}

				// sample rate successfully parsed
				m.Samplerate = samplerate
			} else if len(segment) > 0 && segment[0] == '#' {
				tags := strings.Split(segment[1:], ",")
				m.Hostname, m.DeviceName, m.Tags = extractMagicTags(tags)
			}
		}

		metrics = append(metrics, m)
	}

	return metrics, nil
}

func extractMagicTags(tags []string) (string, string, []string) {
	var hostname, deviceName string
	var recombinedTags []string
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if len(tag) > 0 {
			if strings.HasPrefix(tag, "host:") {
				hostname = tag[5:]
			} else if strings.HasPrefix(tag, "device:") {
				deviceName = tag[7:]
			} else {
				recombinedTags = append(recombinedTags, tag)
			}
		}
	}

	return hostname, deviceName, recombinedTags
}
