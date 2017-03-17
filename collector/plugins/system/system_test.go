package system

import (
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
	"github.com/stretchr/testify/assert"
)

func TestNewStats(t *testing.T) {
	conf := plugin.InitConfig{
		"percpu":   true,
		"totalcpu": false,
	}

	stats := NewStats(conf)
	if s, ok := stats.(*Stats); ok {
		assert.True(t, s.cpu.PerCPU)
		assert.False(t, s.cpu.TotalCPU)
	}
}

func TestCollectSystemMetrics(t *testing.T) {
	s := &Stats{}
	hostinfo, err := host.Info()
	assert.NoError(t, err)
	fields := map[string]float64{
		"system.uptime": float64(hostinfo.Uptime),
	}
	testutil.AssertCheckWithMetrics(t, s.collectSystemMetrics, 1, fields, nil, 1)
}

func TestCollectLoadMetrics(t *testing.T) {
	s := &Stats{}
	loadavg, err := load.Avg()
	assert.NoError(t, err)
	fields := map[string]float64{
		"system.load.1":  loadavg.Load1,
		"system.load.5":  loadavg.Load5,
		"system.load.15": loadavg.Load15,
	}
	testutil.AssertCheckWithMetrics(t, s.collectLoadMetrics, 3, fields, nil)
}

func TestCollectCPUMetrics(t *testing.T) {
	var mps MockPS
	defer mps.AssertExpectations(t)

	cts := cpu.TimesStat{
		CPU:       "cpu0",
		User:      3.1,
		System:    8.2,
		Idle:      80.1,
		Nice:      1.3,
		Iowait:    0.2,
		Irq:       0.1,
		Softirq:   0.11,
		Steal:     0.0511,
		Guest:     8.1,
		GuestNice: 0.324,
	}

	cts2 := cpu.TimesStat{
		CPU:       "cpu0",
		User:      11.4,     // increased by 8.3
		System:    10.9,     // increased by 2.7
		Idle:      158.8699, // increased by 78.7699 (for total increase of 100)
		Nice:      2.5,      // increased by 1.2
		Iowait:    0.7,      // increased by 0.5
		Irq:       1.2,      // increased by 1.1
		Softirq:   0.31,     // increased by 0.2
		Steal:     0.2812,   // increased by 0.0001
		Guest:     12.9,     // increased by 4.8
		GuestNice: 2.524,    // increased by 2.2
	}

	mps.On("CPUTimes").Return([]cpu.TimesStat{cts}, nil)

	s := &Stats{
		ps: &mps,
		cpu: &CPUStats{
			PerCPU:   true,
			TotalCPU: true,
		},
	}
	testutil.AssertCheckWithLen(t, s.collectCPUMetrics, 0)

	mps2 := MockPS{}
	mps2.On("CPUTimes").Return([]cpu.TimesStat{cts2}, nil)
	s.ps = &mps2

	fields := map[string]float64{
		"system.cpu.user":       8.3,
		"system.cpu.system":     2.7,
		"system.cpu.idle":       78.7699,
		"system.cpu.nice":       1.2,
		"system.cpu.iowait":     0.5,
		"system.cpu.irq":        1.1,
		"system.cpu.softirq":    0.2,
		"system.cpu.stolen":     0.2301,
		"system.cpu.guest":      4.8,
		"system.cpu.guest_nice": 2.2,
	}
	tags := []string{
		"cpu:cpu0",
	}
	testutil.AssertCheckWithMetrics(t, s.collectCPUMetrics, 10, fields, tags, 0.0005)
}

func TestCollectMemoryMetrics(t *testing.T) {
	var mps MockPS
	defer mps.AssertExpectations(t)

	vms := &mem.VirtualMemoryStat{
		Total:     12400,
		Available: 7600,
		Used:      5000,
		Free:      1235,
		Buffers:   771,
		Cached:    4312,
		// Wired:     134,
		// Shared:    2142,
	}

	mps.On("VMStat").Return(vms, nil)

	sms := &mem.SwapMemoryStat{
		Total:       8123,
		Used:        1232,
		Free:        6412,
		UsedPercent: 12.2,
		Sin:         7,
		Sout:        830,
	}

	mps.On("SwapStat").Return(sms, nil)

	s := &Stats{
		ps: &mps,
	}
	fields := map[string]float64{
		"system.mem.total":        12400 / MB,
		"system.mem.usable":       7600 / MB,
		"system.mem.used":         (12400 - 1235) / MB,
		"system.mem.free":         1235 / MB,
		"system.mem.cached":       4312 / MB,
		"system.mem.buffered":     771 / MB,
		"system.mem.pct_usable":   100 * float64(7600) / float64(12400),
		"system.swap.total":       8123 / MB,
		"system.swap.used":        1232 / MB,
		"system.swap.free":        6412 / MB,
		"system.swap.pct_free":    100 - 12.2,
		"system.swap.swapped_in":  7,
		"system.swap.swapped_out": 830,
	}
	testutil.AssertCheckWithMetrics(t, s.collectMemoryMetrics, 13, fields, nil)
}

func TestCollectNetMetrics(t *testing.T) {
	var mps MockPS
	defer mps.AssertExpectations(t)

	netio := net.IOCountersStat{
		Name:        "eth0",
		BytesSent:   1123,
		BytesRecv:   8734422,
		PacketsSent: 781,
		PacketsRecv: 23456,
		Errin:       832,
		Errout:      8,
		Dropin:      7,
		Dropout:     1,
	}

	netio2 := net.IOCountersStat{
		Name:        "eth0",
		BytesSent:   1323,    // increased by 200
		BytesRecv:   8834422, // increased by 10000
		PacketsSent: 785,     // increased by 4
		PacketsRecv: 23476,   // increased by 20
		Errin:       832,
		Errout:      8,
		Dropin:      7,
		Dropout:     1,
	}

	mps.On("NetIO").Return([]net.IOCountersStat{netio}, nil)

	netprotos := []net.ProtoCountersStat{
		{
			Protocol: "Udp",
			Stats: map[string]int64{
				"InDatagrams": 4655,
				"NoPorts":     892592,
			},
		},
	}
	mps.On("NetProto").Return(netprotos, nil)

	s := &Stats{
		ps: &mps,
	}

	fields := map[string]float64{
		"system.net.udp_indatagrams": 4655,
		"system.net.udp_noports":     892592,
	}
	tags := []string{
		"interface:all",
	}
	testutil.AssertCheckWithMetrics(t, s.collectNetMetrics, 2, fields, tags)

	mps2 := MockPS{}
	mps2.On("NetIO").Return([]net.IOCountersStat{netio2}, nil)
	mps2.On("NetProto").Return(netprotos, nil)
	s2 := &Stats{
		ps: &mps2,
	}

	fields = map[string]float64{
		"system.net.bytes_sent":        200,
		"system.net.bytes_rcvd":        100000,
		"system.net.packets_in.count":  20,
		"system.net.packets_in.error":  0,
		"system.net.packets_out.count": 4,
		"system.net.packets_out.error": 0,
	}
	tags = []string{
		"interface:eth0",
	}
	testutil.AssertCheckWithRateMetrics(t, s.collectNetMetrics, s2.collectNetMetrics, 8, fields, tags)
}

func TestCollectDiskIOMetrics(t *testing.T) {
	var mps MockPS
	defer mps.AssertExpectations(t)

	diskio1 := disk.IOCountersStat{
		ReadCount:    444,
		WriteCount:   2341,
		ReadBytes:    100000,
		WriteBytes:   200000,
		ReadTime:     3123,
		WriteTime:    6087,
		Name:         "sda1",
		IoTime:       123552,
		SerialNumber: "ab-123-ad",
	}

	diskio2 := disk.IOCountersStat{
		ReadCount:    888,    // increased by 444
		WriteCount:   5341,   // increased by 3000
		ReadBytes:    200000, // increased by 100000
		WriteBytes:   400000, // increased by 200000
		ReadTime:     7123,   // increased by 4000
		WriteTime:    9087,   // increased by 3000
		Name:         "sda1",
		IoTime:       246552, // increased by 123000
		SerialNumber: "ab-123-ad",
	}

	mps.On("DiskIO").Return(map[string]disk.IOCountersStat{"sda1": diskio1}, nil)

	io := &DiskIOStats{}
	s := &Stats{
		ps: &mps,
		io: io,
	}
	testutil.AssertCheckWithLen(t, s.collectDiskIOMetrics, 0)

	mps2 := MockPS{}
	mps2.On("DiskIO").Return(map[string]disk.IOCountersStat{"sda1": diskio2}, nil)
	s2 := &Stats{
		ps: &mps2,
		io: io,
	}

	fields := map[string]float64{
		"system.io.r_s":     444,
		"system.io.w_s":     3000,
		"system.io.rkb_s":   100000 / KB,
		"system.io.wkb_s":   200000 / KB,
		"system.io.r_await": 4000,
		"system.io.w_await": 3000,
		"system.io.util":    123000,
	}
	tags := []string{
		"device:sda1",
	}
	testutil.AssertCheckWithRateMetrics(t, s.collectDiskIOMetrics, s2.collectDiskIOMetrics, 8, fields, tags)

	fields = map[string]float64{
		"system.io.await": float64(4000*444+3000*3000) / float64(444+3000),
	}
	testutil.AssertCheckWithRateMetrics(t, s.collectDiskIOMetrics, s2.collectDiskIOMetrics, 8, fields, nil)
}
