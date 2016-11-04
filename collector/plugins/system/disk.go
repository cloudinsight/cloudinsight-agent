package system

import (
	"fmt"

	"github.com/cloudinsight/cloudinsight-agent/collector"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
)

// NewDiskStats XXX
func NewDiskStats(conf plugin.InitConfig) plugin.Plugin {
	return &DiskStats{
		ps: &systemPS{},
	}
}

// DiskStats XXX
type DiskStats struct {
	ps PS

	MountPoints []string
	IgnoreFS    []string
}

// Check XXX
func (s *DiskStats) Check(agg metric.Aggregator) error {
	disks, err := s.ps.DiskUsage(s.MountPoints, s.IgnoreFS)
	if err != nil {
		return fmt.Errorf("error getting disk usage info: %s", err)
	}

	for _, du := range disks {
		if du.Total == 0 {
			// Skip dummy filesystem (procfs, cgroupfs, ...)
			continue
		}

		var usedPercent float64
		if du.Used+du.Free > 0 {
			usedPercent = float64(du.Used) / (float64(du.Used) + float64(du.Free))
		}

		fields := map[string]interface{}{
			"disk.total":       float64(du.Total) / KB,
			"disk.free":        float64(du.Free) / KB,
			"disk.used":        float64(du.Used) / KB,
			"disk.in_use":      usedPercent,
			"fs.inodes.total":  du.InodesTotal,
			"fs.inodes.free":   du.InodesFree,
			"fs.inodes.used":   du.InodesUsed,
			"fs.inodes.in_use": du.InodesUsedPercent / 100,
		}

		tags := []string{
			"path:" + du.Path,
			"fstype:" + du.Fstype,
		}
		deviceName := du.Path
		agg.AddMetrics("gauge", "system", fields, tags, deviceName)
	}

	return nil
}

func init() {
	collector.Add("disk", NewDiskStats)
}
