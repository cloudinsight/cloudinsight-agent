package docker

import (
	"context"
	"io/ioutil"
	"strings"

	"github.com/docker/docker/api/types"
)

// FakeDockerClient XXX
type FakeDockerClient struct {
}

// ContainerList XXX
func (d FakeDockerClient) ContainerList(octx context.Context, options types.ContainerListOptions) ([]types.Container, error) {
	container1 := types.Container{
		ID:      "e2173b9478a6ae55e237d4d74f8bbb753f0817192b5081334dc78476296b7dfb",
		Names:   []string{"/etcd"},
		Image:   "quay.io/coreos/etcd:v2.2.2",
		Command: "/etcd -name etcd0 -advertise-client-urls http://localhost:2379 -listen-client-urls http://0.0.0.0:2379",
		Created: 1455941930,
		Status:  "Up 4 hours",
		Ports: []types.Port{
			types.Port{
				PrivatePort: 7001,
				PublicPort:  0,
				Type:        "tcp",
			},
			types.Port{
				PrivatePort: 4001,
				PublicPort:  0,
				Type:        "tcp",
			},
			types.Port{
				PrivatePort: 2380,
				PublicPort:  0,
				Type:        "tcp",
			},
			types.Port{
				PrivatePort: 2379,
				PublicPort:  2379,
				Type:        "tcp",
				IP:          "0.0.0.0",
			},
		},
		Labels: map[string]string{
			"com.docker.compose.service": "cloudinsight",
		},
		SizeRw:     0,
		SizeRootFs: 0,
	}
	container2 := types.Container{
		ID:      "b7dfbb9478a6ae55e237d4d74f8bbb753f0817192b5081334dc78476296e2173",
		Names:   []string{"/etcd2"},
		Image:   "quay.io:4443/coreos/etcd:v2.2.2",
		Command: "/etcd -name etcd2 -advertise-client-urls http://localhost:2379 -listen-client-urls http://0.0.0.0:2379",
		Created: 1455941933,
		Status:  "Up 4 hours",
		Ports: []types.Port{
			types.Port{
				PrivatePort: 7002,
				PublicPort:  0,
				Type:        "tcp",
			},
			types.Port{
				PrivatePort: 4002,
				PublicPort:  0,
				Type:        "tcp",
			},
			types.Port{
				PrivatePort: 2381,
				PublicPort:  0,
				Type:        "tcp",
			},
			types.Port{
				PrivatePort: 2382,
				PublicPort:  2382,
				Type:        "tcp",
				IP:          "0.0.0.0",
			},
		},
		Labels: map[string]string{
			"com.docker.compose.service": "cloudinsight",
		},
		SizeRw:     0,
		SizeRootFs: 0,
	}

	containers := []types.Container{container1, container2}
	return containers, nil

	//#{e6a96c84ca91a5258b7cb752579fb68826b68b49ff957487695cd4d13c343b44 titilambert/snmpsim /bin/sh -c 'snmpsimd --agent-udpv4-endpoint=0.0.0.0:31161 --process-user=root --process-group=user' 1455724831 Up 4 hours [{31161 31161 udp 0.0.0.0}] 0 0 [/snmp] map[]}]2016/02/24 01:05:01 Gathered metrics, (3s interval), from 1 inputs in 1.233836656s
}

// ContainerStats XXX
func (d FakeDockerClient) ContainerStats(ctx context.Context, containerID string, stream bool) (types.ContainerStats, error) {
	var stat types.ContainerStats
	jsonStat := `{"read":"2016-02-24T11:42:27.472459608-05:00","memory_stats":{"stats":{},"limit":18935443456},"blkio_stats":{"io_service_bytes_recursive":[{"major":252,"minor":1,"op":"Read","value":753664},{"major":252,"minor":1,"op":"Write"},{"major":252,"minor":1,"op":"Sync"},{"major":252,"minor":1,"op":"Async","value":753664},{"major":252,"minor":1,"op":"Total","value":753664}],"io_serviced_recursive":[{"major":252,"minor":1,"op":"Read","value":26},{"major":252,"minor":1,"op":"Write"},{"major":252,"minor":1,"op":"Sync"},{"major":252,"minor":1,"op":"Async","value":26},{"major":252,"minor":1,"op":"Total","value":26}]},"cpu_stats":{"cpu_usage":{"percpu_usage":[17871,4959158,1646137,1231652,11829401,244656,369972,0],"usage_in_usermode":10000000,"total_usage":20298847},"system_cpu_usage":24052607520000000,"throttling_data":{}},"precpu_stats":{"cpu_usage":{"percpu_usage":[17871,4959158,1646137,1231652,11829401,244656,369972,0],"usage_in_usermode":10000000,"total_usage":20298847},"system_cpu_usage":24052599550000000,"throttling_data":{}}}`
	stat.Body = ioutil.NopCloser(strings.NewReader(jsonStat))
	return stat, nil
}

// ImageList XXX
func (d FakeDockerClient) ImageList(ctx context.Context, options types.ImageListOptions) ([]types.ImageSummary, error) {
	image := types.ImageSummary{
		ID: "4cdd17613acc5e2e590682db14c8df18a911e8e5d932862bb40adc800b797113",
		RepoTags: []string{
			"quay.io:4443/coreos/etcd:v2.2.2",
		},
		Size:        0,
		VirtualSize: 0,
	}
	images := []types.ImageSummary{image}
	return images, nil
}
