package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/collector"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/metric"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
	"github.com/cloudinsight/cloudinsight-agent/common/util"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

// The entity object for the custom tags
const (
	CONTAINER   = "container"
	PERFORMANCE = "performance"
	FILTERED    = "filtered"
	IMAGE       = "image"
)

var (
	DEFAULT_CONTAINER_TAGS = []string{
		"docker_image",
		"image_name",
		"image_tag",
	}

	DEFAULT_PERFORMANCE_TAGS = []string{
		"container_name",
		"docker_image",
		"image_name",
		"image_tag",
	}

	DEFAULT_IMAGE_TAGS = []string{
		"image_name",
		"image_tag",
	}
)

// NewDocker XXX
func NewDocker(conf plugin.InitConfig) plugin.Plugin {
	var timeout int64
	if val, ok := conf["timeout"].(int64); ok {
		timeout = val
	} else {
		timeout = 5
	}

	return &Docker{
		timeout:            timeout,
		tagNames:           make(map[string][]string),
		excludePatterns:    make(map[string]bool),
		includePatterns:    make(map[string]bool),
		filteredContainers: make(map[string]bool),
	}
}

// Docker XXX
type Docker struct {
	URL string

	CollectContainerSize bool `yaml:"collect_container_size"`
	CollectImagesStats   bool `yaml:"collect_images_stats"`
	CollectImageSize     bool `yaml:"collect_image_size"`

	Exclude []string
	Include []string

	Tags                []string
	PerformanceTags     []string `yaml:"performance_tags"`
	ContainerTags       []string `yaml:"container_tags"`
	CollectLabelsAsTags []string `yaml:"collect_labels_as_tags"`

	timeout int64

	client *client.Client

	testing  bool
	tagNames map[string][]string

	filteringEnabled   bool
	excludePatterns    map[string]bool
	includePatterns    map[string]bool
	filteredContainers map[string]bool
}

// Check XXX
func (d *Docker) Check(agg metric.Aggregator) error {
	if d.client == nil && !d.testing {
		var c *client.Client
		var err error
		defaultHeaders := map[string]string{"User-Agent": "engine-api-cli-1.0"}
		if d.URL == "" {
			c, err = client.NewClient("unix:///var/run/docker.sock", "", nil, defaultHeaders)
			if err != nil {
				return err
			}
		} else {
			c, err = client.NewClient(d.URL, "", nil, defaultHeaders)
			if err != nil {
				return err
			}
		}
		d.client = c
	}

	containerTags := DEFAULT_CONTAINER_TAGS
	if d.ContainerTags != nil {
		containerTags = d.ContainerTags
	}
	performanceTags := DEFAULT_PERFORMANCE_TAGS
	if d.PerformanceTags != nil {
		performanceTags = d.PerformanceTags
	}
	d.tagNames = map[string][]string{
		CONTAINER:   containerTags,
		PERFORMANCE: performanceTags,
		IMAGE:       DEFAULT_IMAGE_TAGS,
	}
	d.setFilters()

	// List containers
	opts := types.ContainerListOptions{
		All: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.timeout)*time.Second)
	defer cancel()
	containers, err := listWrapper(d.client, ctx, opts)
	if err != nil {
		return err
	}

	// Filter containers according to the exclude/include rules
	d.filterContainers(containers)

	// Get container data
	var wg sync.WaitGroup
	wg.Add(len(containers))
	for _, container := range containers {
		go func(c types.Container) {
			defer wg.Done()
			err := d.collectContainer(c, agg)
			if err != nil {
				log.Errorf("Error collecting container %s stats: %s",
					c.Names, err.Error())
			}
		}(container)
	}
	wg.Wait()

	if d.CollectImagesStats {
		err := d.collectImageStats(agg)
		if err != nil {
			log.Errorf("Error collecting image stats: %s", err.Error())
		}
	}

	return nil
}

func (d *Docker) collectContainer(
	container types.Container,
	agg metric.Aggregator,
) error {
	var v *types.StatsJSON

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.timeout)*time.Second)
	defer cancel()
	r, err := statsWrapper(d.client, ctx, container.ID, false)
	if err != nil {
		return fmt.Errorf("Error getting docker stats: %s", err.Error())
	}
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	if err = dec.Decode(&v); err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("Error decoding: %s", err.Error())
	}

	tags := d.getTags(container, CONTAINER)
	performanceTags := d.getTags(container, PERFORMANCE)
	isRunning := isContainerRunning(container)
	isExcluded := d.isContainerExcluded(container)
	var runningCount, stoppedCount float64
	if isRunning {
		runningCount = 1
	} else {
		stoppedCount = 1
	}
	agg.Add("count", metric.NewMetric("docker.containers.running", runningCount, tags))
	agg.Add("count", metric.NewMetric("docker.containers.stopped", stoppedCount, tags))

	if isExcluded {
		cname := extractContainerName(container)
		log.Debugf("Container %s is excluded", cname)
		return nil
	}

	if isRunning {
		collectContainerStats(v, agg, performanceTags, container.ID)
	}

	if d.CollectContainerSize {
		agg.Add("gauge", metric.NewMetric("docker.container.size_rw", container.SizeRw, performanceTags))
		agg.Add("gauge", metric.NewMetric("docker.container.size_rootfs", container.SizeRootFs, performanceTags))
	}

	return nil
}

func collectContainerStats(
	stat *types.StatsJSON,
	agg metric.Aggregator,
	tags []string,
	id string,
) {
	now := stat.Read.UnixNano()

	cache := stat.MemoryStats.Stats["cache"]
	rss := stat.MemoryStats.Stats["rss"]
	swap := stat.MemoryStats.Stats["swap"]
	limit := stat.MemoryStats.Stats["hierarchical_memory_limit"]
	swLimit := stat.MemoryStats.Stats["hierarchical_memsw_limit"]
	var inUse, swInUse float64
	if limit > 0 {
		inUse = float64(rss) / float64(limit)
	}
	if swLimit > 0 {
		swInUse = float64(swap+rss) / float64(swLimit)
	}

	memfields := map[string]interface{}{
		"cache":     cache,
		"rss":       rss,
		"swap":      swap,
		"limit":     limit,
		"sw_limit":  swLimit,
		"in_use":    inUse,
		"sw_in_use": swInUse,
	}
	agg.AddMetrics("gauge", "docker.mem", memfields, tags, "", now)

	cpufields := map[string]interface{}{
		"user":   stat.CPUStats.CPUUsage.UsageInUsermode,
		"system": stat.CPUStats.CPUUsage.UsageInKernelmode,
	}
	agg.AddMetrics("rate", "docker.cpu", cpufields, tags, "", now)

	blkioStats := stat.BlkioStats
	var readBytes, writeBytes uint64
	for _, entry := range blkioStats.IoServiceBytesRecursive {
		if entry.Op == "Read" {
			readBytes = entry.Value
		}
		if entry.Op == "Write" {
			writeBytes = entry.Value
		}
	}
	blkiofields := map[string]interface{}{
		"read_bytes":  readBytes,
		"write_bytes": writeBytes,
	}
	agg.AddMetrics("rate", "docker.io", blkiofields, tags, "", now)
}

func (d *Docker) collectImageStats(agg metric.Aggregator) error {
	opts := types.ImageListOptions{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(d.timeout)*time.Second)
	defer cancel()
	activeImages, err := imageListWrapper(d.client, ctx, opts)
	if err != nil {
		return err
	}
	opts.All = true
	allImages, err := imageListWrapper(d.client, ctx, opts)
	if err != nil {
		return err
	}
	agg.Add("gauge", metric.NewMetric("docker.images.available", len(activeImages), d.Tags))
	agg.Add("gauge", metric.NewMetric("docker.images.intermediate", len(allImages)-len(activeImages), d.Tags))

	if d.CollectImageSize {
		var wg sync.WaitGroup
		wg.Add(len(activeImages))
		for _, image := range activeImages {
			go func(img types.ImageSummary) {
				defer wg.Done()

				tags := d.getTags(img, IMAGE)
				agg.Add("gauge", metric.NewMetric("docker.image.virtual_size", img.VirtualSize, tags))
				agg.Add("gauge", metric.NewMetric("docker.image.size", img.Size, tags))
			}(image)
		}
		wg.Wait()
	}

	return nil
}

func (d *Docker) getTags(entity interface{}, tagType string) []string {
	tags := d.Tags
	if entity != nil {
		// Add labels to tags
		var labels map[string]string
		var container types.Container
		var image types.ImageSummary
		var isContainer bool
		if val, ok := entity.(types.Container); ok {
			container = val
			labels = container.Labels
			isContainer = true
		}
		if val, ok := entity.(types.ImageSummary); ok {
			image = val
			labels = image.Labels
		}
		if len(labels) > 0 {
			for _, k := range d.CollectLabelsAsTags {
				if val, ok := labels[k]; ok {
					tags = append(tags, k+":"+val)
				}
			}
		}
		tagNames := d.tagNames[tagType]
		for _, tagName := range tagNames {
			switch tagName {
			case "docker_image":
				tags = append(tags, fmt.Sprintf("%s:%s", tagName, container.Image))
			case "image_name", "image_tag":
				var imageTags []string
				if isContainer {
					imageTags = extractImageTags([]string{container.Image}, tagName)
				} else {
					imageTags = extractImageTags(image.RepoTags, tagName)
				}
				tags = append(tags, imageTags...)
			case "container_command":
				tags = append(tags, fmt.Sprintf("%s:%s", tagName, container.Command))
			case "container_name":
				cname := extractContainerName(container)
				tags = append(tags, fmt.Sprintf("%s:%s", tagName, cname))
			default:
				if tagType != FILTERED {
					log.Warnf("%s isn't a supported tag", tagName)
				}
			}
		}
	}

	return tags
}

func extractContainerName(
	container types.Container,
) string {
	if len(container.Names) > 0 {
		// Not sure what to do with other names, just take the first.
		return strings.TrimPrefix(container.Names[0], "/")
	}
	return container.ID[:11]
}

func extractImageTags(ts []string, tagName string) []string {
	var tags []string
	for _, t := range ts {
		// the image name sometimes has a version part, or a private repo
		//   ie, rabbitmq:3-management or docker.someco.net:4443/rabbitmq:3-management
		i := strings.LastIndex(t, ":") // index of last ':' character
		var tag string
		if i > -1 {
			if tagName == "image_tag" {
				tag = fmt.Sprintf("%s:%s", tagName, t[i+1:])
			} else {
				tag = fmt.Sprintf("%s:%s", tagName, t[:i])
			}
		} else if tagName == "image_name" {
			tag = fmt.Sprintf("%s:%s", tagName, t)
		}
		if !util.StringInSlice(tag, tags) {
			tags = append(tags, tag)
		}
	}
	return tags
}

func (d *Docker) setFilters() {
	if d.Exclude == nil {
		if d.Include != nil {
			log.Warnln("You must specify an exclude section to enable filtering")
		}
		return
	}
	d.filteringEnabled = true

	var filteredTagNames []string
	for _, rule := range d.Exclude {
		d.excludePatterns[rule] = true
		tag := strings.Split(rule, ":")[0]
		if !util.StringInSlice(tag, filteredTagNames) {
			filteredTagNames = append(filteredTagNames, tag)
		}
	}
	for _, rule := range d.Include {
		d.includePatterns[rule] = true
		tag := strings.Split(rule, ":")[0]
		if !util.StringInSlice(tag, filteredTagNames) {
			filteredTagNames = append(filteredTagNames, tag)
		}
	}
	d.tagNames[FILTERED] = filteredTagNames
}

func (d *Docker) isContainerExcluded(
	container types.Container,
) bool {
	// Check if a container is excluded according to the filter rules.
	// Requires filterContainers to run first.
	cname := extractContainerName(container)
	if _, ok := d.filteredContainers[cname]; ok {
		return true
	}
	return false
}

func (d *Docker) filterContainers(
	containers []types.Container,
) {
	if !d.filteringEnabled {
		return
	}

	d.filteredContainers = make(map[string]bool)
	for _, container := range containers {
		tags := d.getTags(container, FILTERED)
		if d.areTagsFiltered(tags) {
			cname := extractContainerName(container)
			d.filteredContainers[cname] = true
			log.Debugf("Container %s is filtered", container.Names[0])
		}
	}
}

func (d *Docker) areTagsFiltered(
	tags []string,
) bool {
	if tagsMatchPatterns(tags, d.excludePatterns) {
		if tagsMatchPatterns(tags, d.includePatterns) {
			return false
		}
		return true
	}
	return false
}

func tagsMatchPatterns(
	tags []string,
	filters map[string]bool,
) bool {
	for rule := range filters {
		for _, tag := range tags {
			match, _ := regexp.MatchString(rule, tag)
			if match {
				return true
			}
		}
	}
	return false
}

// listWrapper wraps client.Client.ContainerList for testing.
func listWrapper(
	c *client.Client,
	ctx context.Context,
	options types.ContainerListOptions,
) ([]types.Container, error) {
	if c != nil {
		return c.ContainerList(ctx, options)
	}
	fc := FakeDockerClient{}
	return fc.ContainerList(ctx, options)
}

// imageListWrapper wraps client.Client.ImageList for testing.
func imageListWrapper(
	c *client.Client,
	ctx context.Context,
	options types.ImageListOptions,
) ([]types.ImageSummary, error) {
	if c != nil {
		return c.ImageList(ctx, options)
	}
	fc := FakeDockerClient{}
	return fc.ImageList(ctx, options)
}

// statsWrapper wraps client.Client.ContainerStats for testing.
func statsWrapper(
	c *client.Client,
	ctx context.Context,
	containerID string,
	stream bool,
) (types.ContainerStats, error) {
	if c != nil {
		return c.ContainerStats(ctx, containerID, stream)
	}
	fc := FakeDockerClient{}
	return fc.ContainerStats(ctx, containerID, stream)
}

func isContainerRunning(
	container types.Container,
) bool {
	// Tell if a container is running, according to its status.
	// There is no "nice" API field to figure it out. We just look at the "Status" field, knowing how it is generated.
	// See: https://github.com/docker/docker/blob/v1.6.2/daemon/state.go#L35
	return strings.HasPrefix(container.Status, "Up") || strings.HasPrefix(container.Status, "Restarting")
}

func init() {
	collector.Add("docker", NewDocker)
}
