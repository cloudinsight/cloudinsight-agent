package system

import (
	"github.com/stretchr/testify/mock"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"

	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
)

// MockPS XXX
type MockPS struct {
	mock.Mock
}

// LoadAvg XXX
func (m *MockPS) LoadAvg() (*load.AvgStat, error) {
	ret := m.Called()

	r0 := ret.Get(0).(*load.AvgStat)
	r1 := ret.Error(1)

	return r0, r1
}

// CPUTimes XXX
func (m *MockPS) CPUTimes(perCPU, totalCPU bool) ([]cpu.TimesStat, error) {
	ret := m.Called()

	r0 := ret.Get(0).([]cpu.TimesStat)
	r1 := ret.Error(1)

	return r0, r1
}

// DiskUsage XXX
func (m *MockPS) DiskUsage(mountPointFilter []string, fstypeExclude []string) ([]*disk.UsageStat, error) {
	ret := m.Called(mountPointFilter, fstypeExclude)

	r0 := ret.Get(0).([]*disk.UsageStat)
	r1 := ret.Error(1)

	return r0, r1
}

// NetIO XXX
func (m *MockPS) NetIO() ([]net.IOCountersStat, error) {
	ret := m.Called()

	r0 := ret.Get(0).([]net.IOCountersStat)
	r1 := ret.Error(1)

	return r0, r1
}

// NetProto XXX
func (m *MockPS) NetProto() ([]net.ProtoCountersStat, error) {
	ret := m.Called()

	r0 := ret.Get(0).([]net.ProtoCountersStat)
	r1 := ret.Error(1)

	return r0, r1
}

// DiskIO XXX
func (m *MockPS) DiskIO() (map[string]disk.IOCountersStat, error) {
	ret := m.Called()

	r0 := ret.Get(0).(map[string]disk.IOCountersStat)
	r1 := ret.Error(1)

	return r0, r1
}

// VMStat XXX
func (m *MockPS) VMStat() (*mem.VirtualMemoryStat, error) {
	ret := m.Called()

	r0 := ret.Get(0).(*mem.VirtualMemoryStat)
	r1 := ret.Error(1)

	return r0, r1
}

// SwapStat XXX
func (m *MockPS) SwapStat() (*mem.SwapMemoryStat, error) {
	ret := m.Called()

	r0 := ret.Get(0).(*mem.SwapMemoryStat)
	r1 := ret.Error(1)

	return r0, r1
}

// NetConnections XXX
func (m *MockPS) NetConnections() ([]net.ConnectionStat, error) {
	ret := m.Called()

	r0 := ret.Get(0).([]net.ConnectionStat)
	r1 := ret.Error(1)

	return r0, r1
}
