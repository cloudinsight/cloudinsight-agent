package config

import (
	"io/ioutil"
	"os"
	"testing"

	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	conf, err := NewConfig("testdata/cloudinsight-agent.conf", nil)
	assert.NoError(t, err)

	expectedConf := &Config{
		GlobalConfig: GlobalConfig{
			CiURL:           "https://dc-cloud.oneapm.com",
			LicenseKey:      "test",
			Hostname:        "test",
			Tags:            "mytag, env:prod, role:database",
			BindHost:        "localhost",
			ListenPort:      9999,
			StatsdPort:      8125,
			NonLocalTraffic: false,
		},
		LoggingConfig: LoggingConfig{
			LogLevel: "debug",
			LogFile:  "/tmp/cloudinsight-agent-testing.log",
		},
	}
	assert.Equal(t, expectedConf.GlobalConfig, conf.GlobalConfig)
	assert.Equal(t, expectedConf.LoggingConfig, conf.LoggingConfig)
}

func TestBadConfig(t *testing.T) {
	_, err := NewConfig("testdata/cloudinsight-agent-bad.conf", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Failed to load the config file:")
}

func TestDefaultConfig(t *testing.T) {
	_, err := NewConfig("testdata/cloudinsight-agent-default.conf", nil)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "LicenseKey must be specified in the config file.")
}

func TestGetForwarderAddr(t *testing.T) {
	conf, _ := NewConfig("testdata/cloudinsight-agent.conf", nil)

	expectedAddr := "localhost:9999"
	assert.Equal(t, expectedAddr, conf.GetForwarderAddr())
}

func TestGetForwarderAddrWithNonLocalTraffic(t *testing.T) {
	conf, _ := NewConfig("testdata/cloudinsight-agent.conf", nil)
	conf.GlobalConfig.NonLocalTraffic = true

	expectedAddr := ":9999"
	assert.Equal(t, expectedAddr, conf.GetForwarderAddr())
}

func TestGetForwarderAddrWithScheme(t *testing.T) {
	conf, _ := NewConfig("testdata/cloudinsight-agent.conf", nil)

	expectedAddr := "http://localhost:9999"
	assert.Equal(t, expectedAddr, conf.GetForwarderAddrWithScheme())
}

func TestGetStatsdAddr(t *testing.T) {
	conf, _ := NewConfig("testdata/cloudinsight-agent.conf", nil)

	expectedAddr := "localhost:8125"
	assert.Equal(t, expectedAddr, conf.GetStatsdAddr())
}

func TestGetStatsdAddrWithNonLocalTraffic(t *testing.T) {
	conf, _ := NewConfig("testdata/cloudinsight-agent.conf", nil)
	conf.GlobalConfig.NonLocalTraffic = true

	expectedAddr := ":8125"
	assert.Equal(t, expectedAddr, conf.GetStatsdAddr())
}

func TestInitializeLogging(t *testing.T) {
	conf, err := NewConfig("testdata/cloudinsight-agent.conf", nil)
	assert.NoError(t, err)
	err = conf.InitializeLogging()
	assert.NoError(t, err)

	log.Debug("This debug-level line should show up in the output.")

	data, err := ioutil.ReadFile(conf.LoggingConfig.LogFile)
	assert.NoError(t, err)
	msg := `level=debug msg="This debug-level line should show up in the output."`
	assert.Contains(t, string(data), msg)
}

func TestInitializeLoggingFailed(t *testing.T) {
	conf, err := NewConfig("testdata/cloudinsight-agent.conf", nil)
	assert.NoError(t, err)
	conf.LoggingConfig.LogLevel = "wrong"
	err = conf.InitializeLogging()
	assert.Error(t, err)
}

func TestGetHostname(t *testing.T) {
	conf, err := NewConfig("testdata/cloudinsight-agent.conf", nil)
	assert.NoError(t, err)
	assert.Equal(t, "test", conf.GetHostname())

	conf.GlobalConfig.Hostname = ""
	expected, _ := os.Hostname()
	assert.Equal(t, expected, conf.GetHostname())
}
