package metric

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Context XXX
type Context [4]string

// Formatter XXX
type Formatter func(metric Metric) interface{}

// Metric XXX
type Metric struct {
	Name           string
	Value          interface{}
	Tags           []string
	Hostname       string
	DeviceName     string
	Timestamp      int64
	LastSampleTime int64
	Type           string
	Samplerate     float64
	Formatter      Formatter
}

// NewMetric XXX
func NewMetric(name string, value interface{}, tags ...[]string) Metric {
	if value == nil {
		panic("Value can't be nil")
	}

	m := Metric{
		Name:  name,
		Value: value,
	}
	if tags != nil {
		m.Tags = tags[0]
	}

	return m
}

func (m *Metric) getCorrectedValue() (float64, error) {
	var value float64

	switch d := m.Value.(type) {
	case int:
		value = float64(int(d))
	case int32:
		value = float64(int32(d))
	case uint32:
		value = float64(uint32(d))
	case int64:
		value = float64(int64(d))
	case uint64:
		value = float64(uint64(d))
	case float32:
		value = float64(d)
	case float64:
		value = float64(d)
	default:
		return 0, fmt.Errorf("undeterminable type: %s", d)
	}

	if math.IsNaN(value) {
		return 0, fmt.Errorf("NaN is an unsupported value for %s", m.Name)
	}

	return value, nil
}

func (m *Metric) removeDuplicates(s []string) []string {
	result := []string{}
	found := map[string]bool{}
	for _, val := range s {
		if _, ok := found[val]; !ok {
			result = append(result, val)
			found[val] = true
		}
	}
	return result
}

func (m *Metric) context() Context {
	tags := m.removeDuplicates(m.Tags)
	sort.Strings(tags)
	return Context{m.Name, strings.Join(tags, ","), m.Hostname, m.DeviceName}
}

// IsExpired XXX
func (m *Metric) IsExpired(expirySeconds int64) bool {
	now := time.Now().Unix()
	if now-m.LastSampleTime > expirySeconds {
		return true
	}
	return false
}

// Format XXX
func (m Metric) Format() interface{} {
	return m.Formatter(m)
}
