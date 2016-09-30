package config

import (
	"io/ioutil"
	"testing"

	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	conf, err := NewConfig("testdata/cloudinsight-agent.conf")
	if err != nil {
		t.Fatalf("couldn't load configuration: %v", err)
	}

	expectedConf := &Config{
		GlobalConfig: GlobalConfig{
			CiURL:      "https://dc-cloud.oneapm.com",
			LicenseKey: "test",
			Hostname:   "test",
			Tags:       "mytag, env:prod, role:database",
			BindHost:   "localhost",
			ListenPort: 9999,
			StatsdPort: 8125,
		},
		LoggingConfig: LoggingConfig{
			LogLevel: "debug",
			LogFile:  "/tmp/cloudinsight-agent-testing.log",
		},
	}
	assert.Equal(t, expectedConf, conf)
}

func TestBadConfig(t *testing.T) {
	_, err := NewConfig("testdata/cloudinsight-agent-bad.conf")
	if err == nil {
		t.Fatal("Expected error but got none")
	}
	assert.Contains(t, err.Error(), "Failed to load the config file:")
}

func TestDefaultConfig(t *testing.T) {
	_, err := NewConfig("testdata/cloudinsight-agent-default.conf")
	if err == nil {
		t.Fatal("Expected error but got none")
	}
	assert.Equal(t, err.Error(), "LicenseKey must be specified in the config file.")
}

func TestGetForwarderAddr(t *testing.T) {
	conf, _ := NewConfig("testdata/cloudinsight-agent.conf")

	expectedAddr := "localhost:9999"
	assert.Equal(t, expectedAddr, conf.GetForwarderAddr())
}

func TestGetForwarderAddrWithScheme(t *testing.T) {
	conf, _ := NewConfig("testdata/cloudinsight-agent.conf")

	expectedAddr := "http://localhost:9999"
	assert.Equal(t, expectedAddr, conf.GetForwarderAddrWithScheme())
}

func TestGetStatsdAddr(t *testing.T) {
	conf, _ := NewConfig("testdata/cloudinsight-agent.conf")

	expectedAddr := "localhost:8125"
	assert.Equal(t, expectedAddr, conf.GetStatsdAddr())
}

func TestInitializeLogging(t *testing.T) {
	conf, _ := NewConfig("testdata/cloudinsight-agent.conf")
	_ = conf.InitializeLogging()

	log.Debug("This debug-level line should show up in the output.")

	data, err := ioutil.ReadFile(conf.LoggingConfig.LogFile)
	if err != nil {
		t.Error(err)
	}
	msg := `level=debug msg="This debug-level line should show up in the output."`
	assert.Contains(t, string(data), msg)
}
