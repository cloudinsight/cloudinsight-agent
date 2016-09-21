package system

import (
	"fmt"
	"strings"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"
	"github.com/startover/cloudinsight-agent/collector"
	"github.com/startover/cloudinsight-agent/common/metric"
	"github.com/startover/cloudinsight-agent/common/plugin"
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

// NewSystemStats XXX
func NewSystemStats(conf plugin.InitConfig) plugin.Plugin {
	var percpu, totalcpu bool
	if val, ok := conf["percpu"].(bool); ok {
		percpu = val
	}
	fmt.Println("percpu:", percpu)
	if val, ok := conf["totalcpu"].(bool); ok {
		totalcpu = val
	}
	fmt.Println("totalcpu:", totalcpu)

	return &SystemStats{
		ps: &systemPS{},
		cpu: &CPUStats{
			PerCPU:   percpu,
			TotalCPU: totalcpu,
		},
		io: &DiskIOStats{},
	}
}

// SystemStats XXX
type SystemStats struct {
	ps  PS
	cpu *CPUStats
	io  *DiskIOStats
}

// CPUStats XXX
type CPUStats struct {
	lastStats []cpu.TimesStat

	PerCPU   bool
	TotalCPU bool
}

// DiskIOStats XXX
type DiskIOStats struct {
	Devices []string
}

func (s *SystemStats) Check(agg metric.Aggregator, instance plugin.Instance) error {
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

func (s *SystemStats) collectSystemMetrics(agg metric.Aggregator) error {
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

func (s *SystemStats) collectLoadMetrics(agg metric.Aggregator) error {
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

func (s *SystemStats) collectCPUMetrics(agg metric.Aggregator) error {
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
			// If it's the 1st gather, can't get CPU Usage stats yet
			continue
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

func (s *SystemStats) collectMemoryMetrics(agg metric.Aggregator) error {
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

func (s *SystemStats) collectNetMetrics(agg metric.Aggregator) error {
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

func (s *SystemStats) collectDiskIOMetrics(agg metric.Aggregator) error {
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

	for _, io := range diskio {
		_, member := devices[io.Name]
		if restrictDevices && !member {
			continue
		}
		var tags []string
		tags = append(tags, "device:"+io.Name)
		var await float64
		if io.ReadCount == 0 {
			await = float64(io.WriteTime)
		} else if io.WriteCount == 0 {
			await = float64(io.ReadTime)
		} else {
			await = float64(io.ReadTime*io.ReadCount+io.WriteTime*io.WriteCount) / float64(io.ReadCount+io.WriteCount)
		}

		fields := map[string]interface{}{
			"r_s":     io.ReadCount,
			"w_s":     io.WriteCount,
			"rkb_s":   float64(io.ReadBytes) / KB,
			"wkb_s":   float64(io.WriteBytes) / KB,
			"r_await": io.ReadTime,
			"w_await": io.WriteTime,
			"await":   await,
			"util":    io.IoTime,
		}
		agg.AddMetrics("rate", "system.io", fields, tags, "")
	}

	return nil
}

func totalCPUTime(t cpu.TimesStat) float64 {
	total := t.User + t.System + t.Nice + t.Iowait + t.Irq + t.Softirq + t.Steal +
		t.Guest + t.GuestNice + t.Idle
	return total
}

func init() {
	collector.Add("system", NewSystemStats)
}
