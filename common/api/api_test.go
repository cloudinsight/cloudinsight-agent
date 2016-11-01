package api

import (
	"bytes"
	"compress/zlib"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewAPI(t *testing.T) {
	api := NewAPI(
		"http://example.com/",
		"dummy-key",
		5*time.Second,
	)

	expectedAPI := &API{
		ciURL:      "http://example.com",
		licenseKey: "dummy-key",
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
	assert.Equal(t, expectedAPI, api)
}

func TestNewAPIWithProxy(t *testing.T) {
	proxy := "http://foo:bar@localhost:8118"
	api := NewAPI(
		"http://example.com/",
		"dummy-key",
		5*time.Second,
		proxy,
	)
	assert.NotNil(t, api.client.Transport)
}

func TestGetURL(t *testing.T) {
	api := NewAPI(
		"http://example.com/",
		"dummy-key",
		5*time.Second,
	)

	for _, c := range []struct {
		in   string
		want string
	}{
		{"metrics", "http://example.com/infrastructure/metrics?license_key=dummy-key"},
		{"service_checks", "http://example.com/infrastructure/service_checks?license_key=dummy-key"},
		{"series", "http://example.com/infrastructure/series?license_key=dummy-key"},
	} {
		out := api.GetURL(c.in)
		if out != c.want {
			t.Errorf("GetURL(%s) == %s, want %s", c.in, out, c.want)
		}
	}
}

func TestPost(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		// Do nothing
	}))
	defer ts.Close()

	api := NewAPI(
		ts.URL,
		"dummy-key",
		5*time.Second,
	)

	err := api.Post(ts.URL, nil)
	assert.NoError(t, err)
}

func TestCompress(t *testing.T) {
	data := `{
		"series": [
			{
			"device_name": "",
			"host": "golang-beta",
			"interval": 10,
			"metric": "golang.beta.test",
			"points": [
				[
				1477723892,
				5
				]
			],
			"tags": null,
			"type": "gauge"
			}
		]}`

	compressed := compress([]byte(data))

	var out bytes.Buffer
	r, _ := zlib.NewReader(&compressed)
	_, _ = io.Copy(&out, r)
	_ = r.Close()

	assert.Equal(t, []byte(data), out.Bytes())
}
