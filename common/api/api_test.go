package api

import (
	"net/http"
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
