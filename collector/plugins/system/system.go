package system

import (
	"fmt"
	"strings"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/collector"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"
)

// KB, MB, GB, TB, PB...human friendly
const (
	_          = iota // ignore first value by assigning to blank identifier
	KB float64 = 1 << (10 * iota)
	MB
	GB
	TB
	PB
)

// NewStats XXX
func NewStats(conf plugin.InitConfig) plugin.Plugin {
	var percpu, totalcpu bool
	if val, ok := conf["percpu"].(bool); ok {
		percpu = val
	}
	if val, ok := conf["totalcpu"].(bool); ok {
		totalcpu = val
	}

	return &Stats{
		ps: &systemPS{},
		cpu: &CPUStats{
			PerCPU:   percpu,
			TotalCPU: totalcpu,
		},
		io: &DiskIOStats{},
	}
}

// Stats XXX
type Stats struct {
	ps  PS
	cpu *CPUStats
	io  *DiskIOStats
}

// CPUStats XXX
type CPUStats struct {
	lastStats []cpu.TimesStat
	PerCPU    bool
	TotalCPU  bool
}

// DiskIOStats XXX
type DiskIOStats struct {
	lastIOStats        map[string]disk.IOCountersStat
	lastCollectionTime int64
	Devices            []string
}

// Check XXX
func (s *Stats) Check(agg metric.Aggregator) error {
	if err := s.collectSystemMetrics(agg); err != nil {
		return err
	}

	if err := s.collectLoadMetrics(agg); err != nil {
		return err
	}

	if err := s.collectCPUMetrics(agg); err != nil {
		return err
	}

	if err := s.collectMemoryMetrics(agg); err != nil {
		return err
	}

	if err := s.collectNetMetrics(agg); err != nil {
		return err
	}

	if err := s.collectDiskIOMetrics(agg); err != nil {
		return err
	}

	return nil
}

func (s *Stats) collectSystemMetrics(agg metric.Aggregator) error {
	hostinfo, err := host.Info()
	if err != nil {
		return err
	}

	agg.Add("gauge", metric.Metric{
		Name:  "system.uptime",
		Value: hostinfo.Uptime,
	})

	return nil
}

func (s *Stats) collectLoadMetrics(agg metric.Aggregator) error {
	loadavg, err := load.Avg()
	if err != nil {
		return err
	}

	fields := map[string]interface{}{
		"1":  loadavg.Load1,
		"5":  loadavg.Load5,
		"15": loadavg.Load15,
	}
	agg.AddMetrics("gauge", "system.load", fields, nil, "")

	return nil
}

func (s *Stats) collectCPUMetrics(agg metric.Aggregator) error {
	times, err := s.ps.CPUTimes(s.cpu.PerCPU, s.cpu.TotalCPU)
	if err != nil {
		return fmt.Errorf("error getting CPU info: %s", err)
	}

	for i, cts := range times {
		tags := []string{
			"cpu:" + cts.CPU,
		}

		total := totalCPUTime(cts)

		// Add in percentage
		if len(s.cpu.lastStats) == 0 {
			// If it's the 1st check, can't get CPU Usage stats yet
			break
		}
		lastCts := s.cpu.lastStats[i]
		lastTotal := totalCPUTime(lastCts)
		totalDelta := total - lastTotal

		if totalDelta < 0 {
			s.cpu.lastStats = times
			return fmt.Errorf("Error: current total CPU time is less than previous total CPU time")
		}

		if totalDelta == 0 {
			continue
		}

		fields := map[string]interface{}{
			"user":       100 * (cts.User - lastCts.User) / totalDelta,
			"system":     100 * (cts.System - lastCts.System) / totalDelta,
			"idle":       100 * (cts.Idle - lastCts.Idle) / totalDelta,
			"nice":       100 * (cts.Nice - lastCts.Nice) / totalDelta,
			"iowait":     100 * (cts.Iowait - lastCts.Iowait) / totalDelta,
			"irq":        100 * (cts.Irq - lastCts.Irq) / totalDelta,
			"softirq":    100 * (cts.Softirq - lastCts.Softirq) / totalDelta,
			"stolen":     100 * (cts.Steal - lastCts.Steal) / totalDelta,
			"guest":      100 * (cts.Guest - lastCts.Guest) / totalDelta,
			"guest_nice": 100 * (cts.GuestNice - lastCts.GuestNice) / totalDelta,
		}
		agg.AddMetrics("gauge", "system.cpu", fields, tags, "")
	}

	s.cpu.lastStats = times

	return nil
}

func (s *Stats) collectMemoryMetrics(agg metric.Aggregator) error {
	vm, err := s.ps.VMStat()
	if err != nil {
		return fmt.Errorf("error getting virtual memory info: %s", err)
	}

	fields := map[string]interface{}{
		"total":      float64(vm.Total) / MB,
		"usable":     float64(vm.Available) / MB,
		"used":       float64(vm.Total-vm.Free) / MB,
		"free":       float64(vm.Free) / MB,
		"cached":     float64(vm.Cached) / MB,
		"buffered":   float64(vm.Buffers) / MB,
		"pct_usable": 100 * float64(vm.Available) / float64(vm.Total),
	}
	agg.AddMetrics("gauge", "system.mem", fields, nil, "")

	swap, err := s.ps.SwapStat()
	if err != nil {
		return fmt.Errorf("error getting swap memory info: %s", err)
	}

	fields = map[string]interface{}{
		"total":       float64(swap.Total) / MB,
		"used":        float64(swap.Used) / MB,
		"free":        float64(swap.Free) / MB,
		"pct_free":    100 - swap.UsedPercent,
		"swapped_in":  swap.Sin,
		"swapped_out": swap.Sout,
	}
	agg.AddMetrics("gauge", "system.swap", fields, nil, "")

	return nil
}

func (s *Stats) collectNetMetrics(agg metric.Aggregator) error {
	netio, err := s.ps.NetIO()
	if err != nil {
		return fmt.Errorf("error getting net io info: %s", err)
	}

	for _, io := range netio {
		tags := []string{
			"interface:" + io.Name,
		}

		fields := map[string]interface{}{
			"bytes_sent":        io.BytesSent,
			"bytes_rcvd":        io.BytesRecv,
			"packets_in.count":  io.PacketsRecv,
			"packets_in.error":  io.Errin + io.Dropin,
			"packets_out.count": io.PacketsSent,
			"packets_out.error": io.Errout + io.Dropout,
		}
		agg.AddMetrics("rate", "system.net", fields, tags, "")
	}

	// Get system wide stats for different network protocols
	// (ignore these stats if the call fails)
	netprotos, _ := s.ps.NetProto()
	fields := make(map[string]interface{})
	for _, proto := range netprotos {
		for stat, value := range proto.Stats {
			name := fmt.Sprintf("%s_%s", strings.ToLower(proto.Protocol),
				strings.ToLower(stat))
			fields[name] = value
		}
	}
	tags := []string{
		"interface:all",
	}
	agg.AddMetrics("gauge", "system.net", fields, tags, "")

	return nil
}

func (s *Stats) collectDiskIOMetrics(agg metric.Aggregator) error {
	diskio, err := s.ps.DiskIO()
	if err != nil {
		return fmt.Errorf("error getting disk io info: %s", err)
	}

	var restrictDevices bool
	devices := make(map[string]bool)
	if len(s.io.Devices) != 0 {
		restrictDevices = true
		for _, dev := range s.io.Devices {
			devices[dev] = true
		}
	}

	for name, io := range diskio {
		_, member := devices[io.Name]
		if restrictDevices && !member {
			continue
		}
		var tags []string
		tags = append(tags, "device:"+io.Name)

		fields := map[string]interface{}{
			"r_s":     io.ReadCount,
			"w_s":     io.WriteCount,
			"rkb_s":   float64(io.ReadBytes) / KB,
			"wkb_s":   float64(io.WriteBytes) / KB,
			"r_await": io.ReadTime,
			"w_await": io.WriteTime,
			"util":    io.IoTime,
		}
		agg.AddMetrics("rate", "system.io", fields, tags, "")

		if len(s.io.lastIOStats) == 0 {
			// If it's the 1st check, can't get ioawait yet
			continue
		}

		// Compute ioawait
		lastStats := s.io.lastIOStats[name]
		timeDelta := time.Now().Unix() - s.io.lastCollectionTime
		if timeDelta == 0 {
			continue
		}

		ioReadTimeDelta := float64(io.ReadTime-lastStats.ReadTime) / float64(timeDelta)
		ioWriteTimeDelta := float64(io.WriteTime-lastStats.WriteTime) / float64(timeDelta)
		ioReadCountDelta := float64(io.ReadCount-lastStats.ReadCount) / float64(timeDelta)
		ioWriteCountDelta := float64(io.WriteCount-lastStats.WriteCount) / float64(timeDelta)

		var ioawait float64
		if ioReadCountDelta == 0 {
			ioawait = ioWriteTimeDelta
		} else if ioWriteCountDelta == 0 {
			ioawait = ioReadTimeDelta
		} else {
			ioawait = float64(ioReadTimeDelta*ioReadCountDelta+
				ioWriteTimeDelta*ioWriteCountDelta) / float64(ioReadCountDelta+ioWriteCountDelta)
		}

		agg.Add("gauge", metric.NewMetric("system.io.await", ioawait))
	}

	s.io.lastIOStats = diskio
	s.io.lastCollectionTime = time.Now().Unix()

	return nil
}

func totalCPUTime(t cpu.TimesStat) float64 {
	total := t.User + t.System + t.Nice + t.Iowait + t.Irq + t.Softirq + t.Steal +
		t.Guest + t.GuestNice + t.Idle
	return total
}

func init() {
	collector.Add("system", NewStats)
}
