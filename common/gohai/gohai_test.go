package gohai

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockCollector struct {
}

func (m *mockCollector) Name() string {
	return "mockCollector"
}

func (m *mockCollector) Collect() (interface{}, error) {
	return "mockCollector", nil
}

func TestGetInactivePids(t *testing.T) {
	for _, c := range []struct {
		pids       []int32
		cachedPids []int32
		expected   []int32
	}{
		{[]int32{1, 2, 3}, []int32{1, 2, 4}, []int32{4}},
		{[]int32{1, 2, 3}, []int32{1, 4, 5}, []int32{4, 5}},
		{[]int32{1, 2, 3}, []int32{4, 5, 6}, []int32{4, 5, 6}},
	} {
		out := getInactivePids(c.pids, c.cachedPids)
		assert.Equal(t, c.expected, out)
	}
}

func TestGetMetadata(t *testing.T) {
	collectors = []Collector{
		&mockCollector{},
	}

	metadata := GetMetadata()
	expected := map[string]interface{}{"mockCollector": "mockCollector"}
	assert.Equal(t, expected, metadata)
}

func TestGetProcesses(t *testing.T) {
	getPids = mockPids
	processes := GetProcesses()
	assert.Len(t, processes, 1)
	if field, ok := processes[0].(processField); ok {
		// We can't get the process CPUPercent this time.
		assert.Zero(t, field[2])
	}
}

func mockPids() ([]int32, error) {
	checkPid := os.Getpid()
	return []int32{int32(checkPid)}, nil
}
