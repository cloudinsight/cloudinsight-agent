package agent

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetUUID(t *testing.T) {
	uuid := getUUID()
	uuid2 := getUUID()
	assert.Equal(t, uuid, uuid2)
	assert.Len(t, uuid, 32)
}
