package agent

import (
	"encoding/hex"
	"net"
	"os"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	uuid "github.com/nu7hatch/gouuid"
)

// NewPayload XXX
func NewPayload(conf *config.Config) *Payload {
	p := &Payload{
		AgentVersion:        config.VERSION,
		CollectionTimestamp: time.Now().Unix(),
		InternalHostname:    conf.GetHostname(),
		LicenseKey:          conf.GlobalConfig.LicenseKey,
		UUID:                getUUID(),
	}

	return p
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

func getMacAddr() string {
	interfaces, _ := net.Interfaces()
	for _, inter := range interfaces {
		if inter.HardwareAddr != nil {
			return inter.HardwareAddr.String()
		}
	}

	return ""
}

func getUUID() string {
	hostname, _ := os.Hostname()
	addr := getMacAddr()
	u5, err := uuid.NewV5(uuid.NamespaceDNS, []byte(hostname+addr))
	if err != nil {
		log.Error(err)
		return ""
	}

	return hex.EncodeToString(u5[:])
}
