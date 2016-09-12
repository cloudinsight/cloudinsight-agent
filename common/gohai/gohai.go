package gohai

import (
	"math"

	"github.com/DataDog/gohai/cpu"
	"github.com/DataDog/gohai/filesystem"
	"github.com/DataDog/gohai/memory"
	"github.com/DataDog/gohai/network"
	"github.com/DataDog/gohai/platform"
	"github.com/shirou/gopsutil/process"
	"github.com/startover/cloudinsight-agent/common/log"
)

// Collector XXX
type Collector interface {
	Name() string
	Collect() (interface{}, error)
}

var collectors = []Collector{
	&cpu.Cpu{},
	&filesystem.FileSystem{},
	&memory.Memory{},
	&network.Network{},
	&platform.Platform{},
}

// GetMetadata XXX
func GetMetadata() map[string]interface{} {
	result := make(map[string]interface{})

	for _, collector := range collectors {
		c, err := collector.Collect()
		if err != nil {
			log.Warnf("[%s] %s", collector.Name(), err)
			continue
		}
		result[collector.Name()] = c
	}

	return result
}

var processCache map[int32]*process.Process

func init() {
	processCache = make(map[int32]*process.Process)
}

type processField [11]interface{}

func getInactivePids(pids []int32, cachedPids []int32) []int32 {
	var complement []int32
	for _, pid := range cachedPids {
		if !contains(pids, pid) {
			complement = append(complement, pid)
		}
	}
	return complement
}

func contains(slice []int32, item int32) bool {
	for _, val := range slice {
		if val == item {
			return true
		}
	}
	return false
}

func cast(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func round(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(cast(num*output)) / output
}

// GetProcesses XXX
func GetProcesses() []interface{} {
	var err error
	var processes []interface{}
	var pids []int32
	pids, _ = process.Pids()

	cachedPids := make([]int32, len(processCache))
	i := 0
	for k := range processCache {
		cachedPids[i] = k
		i++
	}

	inactivePids := getInactivePids(pids, cachedPids)
	for _, pid := range inactivePids {
		delete(processCache, pid)
	}

	var username string
	var cpuPercent float64
	var memPercent float32
	var memInfo *process.MemoryInfoStat
	var name string

	for _, pid := range pids {
		if _, ok := processCache[pid]; !ok {
			processCache[pid], err = process.NewProcess(pid)
			if err != nil {
				log.Error(err)
				continue
			}
		}

		p := processCache[pid]

		username, err = p.Username()
		if err != nil {
			log.Error(err)
			continue
		}

		cpuPercent, err = p.Percent(0)
		if err != nil {
			log.Error(err)
			continue
		}

		memPercent, err = p.MemoryPercent()
		if err != nil {
			log.Error(err)
			continue
		}

		memInfo, err = p.MemoryInfo()
		if err != nil {
			log.Error(err)
			continue
		}

		name, err = p.Name()
		if err != nil {
			log.Error(err)
			continue
		}

		processField := processField{
			username,
			pid,
			round(cpuPercent, 2),
			round(float64(memPercent), 2),
			memInfo.VMS,
			memInfo.RSS,
			nil,
			nil,
			nil,
			nil,
			name,
		}
		processes = append(processes, processField)
	}

	return processes
}
