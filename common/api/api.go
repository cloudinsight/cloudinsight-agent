package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

// SubmitMetrics XXX
func (api *API) SubmitMetrics(data interface{}) error {
	// var body bytes.Buffer

	// err := json.NewEncoder(&body).Encode(data)
	tsBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("unable to marshal data, %s\n", err.Error())
	}

	// return api.Post(api.GetURL("metrics"), &body)
	return api.Post(api.GetURL("metrics"), bytes.NewBuffer(tsBytes))
}

// Post XXX
func (api *API) Post(path string, body io.Reader) error {
	req, err := http.NewRequest("POST", path, body)
	if err != nil {
		return fmt.Errorf("unable to create http.Request, %s\n", err.Error())
	}

	resp, err := api.do(req)
	defer closeResp(resp)
	if err != nil {
		return fmt.Errorf("error POSTing data, %s\n", err.Error())
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

// GetURL XXX
func (api *API) GetURL(msgType string) string {
	q := url.Values{
		"license_key": []string{api.LicenseKey},
	}

	switch msgType {
	case "metrics":
		return fmt.Sprintf("%s/infrastructure/metrics?%s", api.CiURL, q.Encode())
	case "service_checks":
		return fmt.Sprintf("%s/infrastructure/service_checks?%s", api.CiURL, q.Encode())
	case "series":
		return fmt.Sprintf("%s/infrastructure/series?%s", api.CiURL, q.Encode())
	}

	return ""
}
