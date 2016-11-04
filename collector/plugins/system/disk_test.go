package system

import (
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common"
	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/shirou/gopsutil/disk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskStats(t *testing.T) {
	var mps MockPS
	defer mps.AssertExpectations(t)
	metricC := make(chan metric.Metric, 50)
	defer close(metricC)
	conf := &config.Config{}
	agg := testutil.MockAggregator(metricC, conf)
	var err error

	duAll := []*disk.UsageStat{
		{
			Path:              "/",
			Fstype:            "ext4",
			Total:             128,
			Free:              23,
			Used:              100,
			InodesTotal:       1234,
			InodesFree:        234,
			InodesUsed:        1000,
			InodesUsedPercent: 81.30081300813008,
		},
		{
			Path:              "/home",
			Fstype:            "ext4",
			Total:             256,
			Free:              46,
			Used:              200,
			InodesTotal:       2468,
			InodesFree:        468,
			InodesUsed:        2000,
			InodesUsedPercent: 81.30081300813008,
		},
	}
	duFiltered := []*disk.UsageStat{
		{
			Path:              "/",
			Fstype:            "ext4",
			Total:             128,
			Free:              23,
			Used:              100,
			InodesTotal:       1234,
			InodesFree:        234,
			InodesUsed:        1000,
			InodesUsedPercent: 81.30081300813008,
		},
	}

	mps.On("DiskUsage", []string(nil), []string(nil)).Return(duAll, nil)
	mps.On("DiskUsage", []string{"/", "/dev"}, []string(nil)).Return(duFiltered, nil)
	mps.On("DiskUsage", []string{"/", "/home"}, []string(nil)).Return(duAll, nil)

	err = (&DiskStats{ps: &mps}).Check(agg)
	require.NoError(t, err)
	agg.Flush()
	expectedAllDiskMetrics := 16
	assert.Len(t, metricC, expectedAllDiskMetrics)

	metrics := make([]metric.Metric, expectedAllDiskMetrics)
	for i := 0; i < expectedAllDiskMetrics; i++ {
		metrics[i] = <-metricC
	}

	fields1 := map[string]float64{
		"system.disk.total":       float64(128) / KB,
		"system.disk.used":        float64(100) / KB,
		"system.disk.free":        float64(23) / KB,
		"system.disk.in_use":      float64(0.8130081300813008),
		"system.fs.inodes.total":  float64(1234),
		"system.fs.inodes.free":   float64(234),
		"system.fs.inodes.used":   float64(1000),
		"system.fs.inodes.in_use": float64(0.8130081300813008),
	}
	tags1 := []string{
		"path:/",
		"fstype:ext4",
	}
	for name, value := range fields1 {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags1)
	}

	tags2 := []string{
		"path:/home",
		"fstype:ext4",
	}
	fields2 := map[string]float64{
		"system.disk.total":       float64(256) / KB,
		"system.disk.used":        float64(200) / KB,
		"system.disk.free":        float64(46) / KB,
		"system.disk.in_use":      float64(0.8130081300813008),
		"system.fs.inodes.total":  float64(2468),
		"system.fs.inodes.free":   float64(468),
		"system.fs.inodes.used":   float64(2000),
		"system.fs.inodes.in_use": float64(0.8130081300813008),
	}
	for name, value := range fields2 {
		testutil.AssertContainsMetricWithTags(t, metrics, name, value, tags2)
	}

	// // We expect 6 more DiskMetrics to show up with an explicit match on "/"
	// // and /home not matching the /dev in MountPoints
	err = (&DiskStats{ps: &mps, MountPoints: []string{"/", "/dev"}}).Check(agg)
	require.NoError(t, err)
	agg.Flush()
	assert.Len(t, metricC, 8)

	// // We should see all the diskpoints as MountPoints includes both
	// // / and /home
	err = (&DiskStats{ps: &mps, MountPoints: []string{"/", "/home"}}).Check(agg)
	require.NoError(t, err)
	agg.Flush()
	assert.Len(t, metricC, expectedAllDiskMetrics+8)
}
