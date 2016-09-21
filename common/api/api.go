package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/startover/cloudinsight-agent/common/log"
)

// API XXX
type API struct {
	CiURL      string
	LicenseKey string
	client     *http.Client
}

// NewAPI XXX
func NewAPI(ciURL string, licenseKey string, timeout time.Duration) *API {
	api := &API{
		CiURL:      ciURL,
		LicenseKey: licenseKey,
		client: &http.Client{
			Timeout: timeout,
		},
	}
	return api
}

// Post XXX
func (api *API) Post(payload *Payload) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("unable to marshal Payload, %s\n", err.Error())
	}

	req, err := http.NewRequest("POST", api.getURL(), bytes.NewBuffer(payloadBytes))
	if err != nil {
		return fmt.Errorf("unable to create http.Request, %s\n", err.Error())
	}

	resp, err := api.do(req)
	defer closeResp(resp)
	if err != nil {
		return fmt.Errorf("error POSTing Payload, %s\n", err.Error())
	}

	if resp.StatusCode < 200 || resp.StatusCode > 209 {
		return fmt.Errorf("received bad status code, %d\n", resp.StatusCode)
	}

	return nil
}

func (api *API) do(req *http.Request) (resp *http.Response, err error) {
	req.Header.Add("Content-Type", "application/json")

	resp, err = api.client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func closeResp(resp *http.Response) {
	if resp != nil {
		err := resp.Body.Close()
		if err != nil {
			log.Errorf("failed to close the HTTP Response, %s\n", err.Error())
		}
	}
}

func (api *API) getURL() string {
	q := url.Values{
		"license_key": []string{api.LicenseKey},
	}
	return fmt.Sprintf("%s/infrastructure/metrics?%s", api.CiURL, q.Encode())
}
