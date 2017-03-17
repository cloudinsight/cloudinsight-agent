package docker

import (
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common"
)

func TestDockerCheck(t *testing.T) {
	d := Docker{
		CollectContainerSize: true,
		CollectImagesStats:   true,
		CollectImageSize:     true,
		Exclude:              []string{".*"},
		Include:              []string{"docker_image:quay.io:4443/coreos/etcd:v2.2.2"},
		Tags:                 []string{"service:docker"},
		CollectLabelsAsTags:  []string{"com.docker.compose.service"},
		tagNames:             make(map[string][]string),
		excludePatterns:      make(map[string]bool),
		includePatterns:      make(map[string]bool),
		filteredContainers:   make(map[string]bool),
		client:               nil,
		testing:              true,
	}
	d2 := d

	fields := map[string]float64{
		"docker.containers.running": 1,
		"docker.containers.stopped": 0,
	}
	tags := []string{
		"service:docker",
		"com.docker.compose.service:cloudinsight",
		"docker_image:quay.io:4443/coreos/etcd:v2.2.2",
		"image_name:quay.io:4443/coreos/etcd",
		"image_tag:v2.2.2",
	}
	testutil.AssertCheckWithMetrics(t, d.Check, 17, fields, tags)

	tags = []string{
		"service:docker",
		"com.docker.compose.service:cloudinsight",
		"docker_image:quay.io/coreos/etcd:v2.2.2",
		"image_name:quay.io/coreos/etcd",
		"image_tag:v2.2.2",
	}
	testutil.AssertCheckWithMetrics(t, d.Check, 17, fields, tags)

	// Container performance stats
	fields = map[string]float64{
		"docker.container.size_rw":     0,
		"docker.container.size_rootfs": 0,
		"docker.mem.cache":             0,
		"docker.mem.rss":               0,
		"docker.mem.swap":              0,
		"docker.mem.limit":             0,
		"docker.mem.sw_limit":          0,
		"docker.mem.in_use":            0,
		"docker.mem.sw_in_use":         0,
	}
	performanceTags := []string{
		"service:docker",
		"com.docker.compose.service:cloudinsight",
		"container_name:etcd2",
		"docker_image:quay.io:4443/coreos/etcd:v2.2.2",
		"image_name:quay.io:4443/coreos/etcd",
		"image_tag:v2.2.2",
	}
	testutil.AssertCheckWithMetrics(t, d.Check, 17, fields, performanceTags)

	fields = map[string]float64{
		"docker.cpu.system":     0,
		"docker.cpu.user":       0,
		"docker.io.read_bytes":  0,
		"docker.io.write_bytes": 0,
	}
	testutil.AssertCheckWithRateMetrics(t, d.Check, d2.Check, 21, fields, performanceTags)

	// Image Stats
	fields = map[string]float64{
		"docker.images.available":    1,
		"docker.images.intermediate": 0,
	}
	imageTags := []string{"service:docker"}
	testutil.AssertCheckWithMetrics(t, d.Check, 17, fields, imageTags)

	// Image Size
	fields = map[string]float64{
		"docker.image.virtual_size": 0,
		"docker.image.size":         0,
	}
	imageTags = []string{"service:docker", "image_name:quay.io:4443/coreos/etcd", "image_tag:v2.2.2"}
	testutil.AssertCheckWithMetrics(t, d.Check, 17, fields, imageTags)
}
