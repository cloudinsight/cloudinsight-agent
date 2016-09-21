package api

import (
	"encoding/hex"
	"time"

	uuid "github.com/nu7hatch/gouuid"
	"github.com/prometheus/common/log"
	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/gohai"
)

// NewPayload XXX
func NewPayload(conf *config.Config, metrics []interface{}) *Payload {
	return &Payload{
		AgentVersion:        config.VERSION,
		CollectionTimestamp: time.Now().Unix(),
		InternalHostname:    conf.GetHostname(),
		LicenseKey:          conf.GlobalConfig.LicenseKey,
		Metrics:             metrics,
		UUID:                getUUID(),
		Gohai:               gohai.GetMetadata(),
		Processes: map[string]interface{}{
			"processes":  gohai.GetProcesses(),
			"licenseKey": conf.GlobalConfig.LicenseKey,
			"host":       "test-golang",
		},
	}
}

// Payload XXX
type Payload struct {
	AgentVersion        string                 `json:"agentVersion"`
	CollectionTimestamp int64                  `json:"collection_timestamp"`
	InternalHostname    string                 `json:"internalHostname"`
	LicenseKey          string                 `json:"licenseKey"`
	Metrics             []interface{}          `json:"metrics,omitempty"`
	ServiceChecks       []interface{}          `json:"service_checks"`
	UUID                string                 `json:"uuid"`
	Gohai               map[string]interface{} `json:"gohai,omitempty"`
	Processes           map[string]interface{} `json:"processes,omitempty"`
	HostTags            map[string]interface{} `json:"host-tags,omitempty"`
	Events              map[string]interface{} `json:"events,omitempty"`
}

func getUUID() string {
	u5, err := uuid.NewV5(uuid.NamespaceDNS, []byte("cloudinsight"))
	if err != nil {
		log.Error(err)
		return ""
	}
	return hex.EncodeToString(u5[:])
}
