package forwarder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/startover/cloudinsight-agent/common/gohai"
	"github.com/startover/cloudinsight-agent/common/log"
)

// API XXX
type API struct {
	ciURL      string
	licenseKey string
	timeout    time.Duration
	client     *http.Client
}

// Payload XXX
type Payload struct {
	AgentVersion        string                 `json:"agentVersion"`
	CollectionTimestamp int64                  `json:"collection_timestamp"`
	InternalHostname    string                 `json:"internalHostname"`
	LicenseKey          string                 `json:"licenseKey"`
	Metrics             []interface{}          `json:"metrics,omitempty"`
	Env                 string                 `json:"env"`
	ServiceChecks       []interface{}          `json:"service_checks"`
	UUID                string                 `json:"uuid"`
	Gohai               map[string]interface{} `json:"gohai,omitempty"`
	Processes           map[string]interface{} `json:"processes,omitempty"`
	HostTags            map[string]interface{} `json:"host-tags,omitempty"`
	Events              map[string]interface{} `json:"events,omitempty"`
	SystemStats         map[string]interface{} `json:"systemStats,omitempty"`
}

// CiURL XXX
const CiURL = "https://dc-cloud.oneapm.com"

// NewAPI XXX
func NewAPI(ciURL string, licenseKey string, timeout time.Duration) *API {
	return &API{
		ciURL:      ciURL,
		licenseKey: licenseKey,
		timeout:    timeout,
	}
}

// NewPayload XXX
func NewPayload(licenseKey string, metrics []interface{}) *Payload {
	return &Payload{
		AgentVersion:        "0.0.1",
		CollectionTimestamp: time.Now().Unix(),
		InternalHostname:    "golang-beta",
		LicenseKey:          licenseKey,
		Metrics:             metrics,
		Env:                 "go1.6.3 linux/amd64",
		UUID:                "954a8d0bd6335eaeb508d9040619dedb",
		Gohai:               gohai.GetMetadata(),
		Processes: map[string]interface{}{
			"processes":  gohai.GetProcesses(),
			"licenseKey": licenseKey,
			"host":       "test-golang",
		},
	}
}

// Connect XXX
func (api *API) Connect() error {
	if api.licenseKey == "" {
		return fmt.Errorf("LicenseKey is required for cloudinsight. You can find it at https://cloud.oneapm.com/#/settings")
	}
	api.client = &http.Client{
		Timeout: api.timeout,
	}
	return nil
}

// Write XXX
func (api *API) Write(payload *Payload) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("unable to marshal Payload, %s\n", err.Error())
	}
	log.Infof("url: %s", api.authenticatedURL())
	// log.Infof("payload: %s", payloadBytes)
	req, err := http.NewRequest("POST", api.authenticatedURL(), bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("unable to create http.Request, %s\n", err.Error())
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := api.client.Do(req)
	if err != nil {
		return fmt.Errorf("error POSTing Payload, %s\n", err.Error())
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			log.Errorf("failed to close the HTTP Response, %s\n", err.Error())
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode > 209 {
		return fmt.Errorf("received bad status code, %d\n", resp.StatusCode)
	}

	return nil
}

func (api *API) authenticatedURL() string {
	q := url.Values{
		"license_key": []string{api.licenseKey},
	}
	return fmt.Sprintf("%s/infrastructure/metrics?%s", api.ciURL, q.Encode())
}
