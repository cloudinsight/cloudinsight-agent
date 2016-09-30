package plugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	conf, err := LoadConfig("testdata/nginx.yaml")
	if err != nil {
		t.Fatalf("couldn't load configuration: %v", err)
	}

	assert.NotNil(t, conf.InitConfig)
	assert.NotNil(t, conf.Instances)

	expectedConf := &Config{
		InitConfig: map[string]interface{}{
			"check_interval": 60,
		},
		Instances: []Instance{
			{
				"nginx_status_url": "http://localhost/nginx_status/",
				"tags":             []interface{}{"foo:bar"},
			},
		},
	}
	assert.Equal(t, expectedConf, conf)
}

func TestBadConfig(t *testing.T) {
	_, err := LoadConfig("testdata/nginx_bad.yaml")
	if err == nil {
		t.Errorf("Expected error parsing %s but got none", "testdata/nginx_bad.yaml")
	}
}

func TestEmptyConfig(t *testing.T) {
	_, err := LoadConfig("")
	if err == nil {
		t.Fatal("Expected error but got none")
	}
	assert.Contains(t, err.Error(), "no such file or directory")
}
