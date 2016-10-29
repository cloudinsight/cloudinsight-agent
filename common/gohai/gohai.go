package gohai

import (
	"github.com/DataDog/gohai/cpu"
	"github.com/DataDog/gohai/filesystem"
	"github.com/DataDog/gohai/memory"
	"github.com/DataDog/gohai/network"
	"github.com/DataDog/gohai/platform"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/util"
	"github.com/shirou/gopsutil/process"
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

// GetMetadata collects system information of cpu, filesystem, memory, network and platform.
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
		if !util.Contains(pids, pid) {
			complement = append(complement, pid)
		}
	}
	return complement
}

// Just make code more testable.
var getPids = process.Pids

// GetProcesses gets the processes list.
func GetProcesses() []interface{} {
	var err error
	var processes []interface{}
	var pids []int32
	pids, _ = getPids()

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
			util.Round(cpuPercent, 2),
			util.Round(float64(memPercent), 2),
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
