package metric

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetCorrectedValue(t *testing.T) {
	m := Metric{
		Name: "test.value",
	}
	for _, val := range []interface{}{
		int(22),
		int32(22),
		uint32(22),
		int64(22),
		uint64(22),
		float32(22),
		float64(22),
	} {
		m.Value = val
		value, err := m.getCorrectedValue()
		assert.NoError(t, err)
		assert.Equal(t, float64(22), value)
	}
}

func TestGetCorrectedValueWithWrongType(t *testing.T) {
	m := Metric{
		Name: "test.value.error",
	}
	for _, val := range []interface{}{
		true,
		"22",
	} {
		m.Value = val
		_, err := m.getCorrectedValue()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "undeterminable type:")
	}
}

func TestGetCorrectedValueWithNaNORInf(t *testing.T) {
	m := Metric{
		Name: "test.value.error",
	}
	for _, val := range []interface{}{
		math.NaN(),
		math.Inf(1),
		math.Inf(-1),
	} {
		m.Value = val
		_, err := m.getCorrectedValue()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "NaN and Inf is unsupported value for ")
	}
}
