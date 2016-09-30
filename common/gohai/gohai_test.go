package gohai

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
