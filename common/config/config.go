package config

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/startover/cloudinsight-agent/collector"
	"github.com/startover/cloudinsight-agent/common/log"
	"github.com/startover/cloudinsight-agent/common/plugin"
)

// NewConfig XXX
func NewConfig() *Config {
	c := &Config{
		GlobalConfig: &GlobalConfig{
			CiURL: "https://dc-cloud.oneapm.com",
		},
	}

	return c
}

// Config represents cloudinsight-agent's configuration file.
type Config struct {
	GlobalConfig  *GlobalConfig  `toml:"global"`
	LoggingConfig *LoggingConfig `toml:"logging"`
	Plugins       []*plugin.RunningPlugin
}

// GlobalConfig XXX
type GlobalConfig struct {
	CiURL      string `toml:"ci_url"`
	LicenseKey string `toml:"license_key"`
}

// LoggingConfig XXX
type LoggingConfig struct {
	LogLevel         string `toml:"log_level"`
	CollectorLogFile string `toml:"collector_log_file"`
	ForwarderLogFile string `toml:"forwarder_log_file"`
	StatsdLogFile    string `toml:"statsd_log_file"`
	LogToSyslog      bool   `toml:"log_to_syslog"`
}

// Try to find a default config file at these locations (in order):
//   1. /etc/cloudinsight-agent/cloudinsight-agent.conf
//   2. $HOME/.cloudinsight-agent/cloudinsight-agent.conf
//
func getDefaultConfigPath() (string, error) {
	etcfile := "/etc/cloudinsight-agent/cloudinsight-agent.conf"
	homefile := os.ExpandEnv("$HOME/.cloudinsight-agent/cloudinsight-agent.conf")
	for _, path := range []string{etcfile, homefile} {
		if _, err := os.Stat(path); err == nil {
			log.Infof("Using config file: %s", path)
			return path, nil
		}
	}

	// if we got here, we didn't find a file in a default location
	return "", fmt.Errorf("No config file specified, and could not find one"+
		" in %s, or %s", etcfile, homefile)
}

// LoadConfig XXX
func (c *Config) LoadConfig(confPath string) error {
	var err error
	if confPath == "" {
		if confPath, err = getDefaultConfigPath(); err != nil {
			return err
		}
	}

	if _, err = toml.DecodeFile(confPath, c); err != nil {
		return err
	}

	patterns := [2]string{"*.yaml", "*.yaml.default"}
	var files []string
	root, err := os.Getwd()
	if err != nil {
		log.Errorf("Failed to get root path %s", err)
		return err
	}
	for _, pattern := range patterns {
		log.Info(filepath.Join(root, "collector/conf.d", pattern))
		m, _ := filepath.Glob(filepath.Join(root, "collector/conf.d", pattern))
		files = append(files, m...)
	}

	for i, file := range files {
		log.Infoln(i, file)
		pluginConfig, err := plugin.LoadConfig(file)
		if err != nil {
			log.Errorf("Failed to parse Plugin Config %s: %s", file, err)
			continue
		}

		filename := path.Base(file)
		pluginName := strings.Split(filename, ".")[0]
		err = c.addPlugin(pluginName, pluginConfig)
		if err != nil {
			log.Errorf("Failed to load Plugin %s: %s", pluginName, err)
			continue
		}
	}

	return nil
}

//InitializeLogging XXX
func (c *Config) InitializeLogging() error {
	err := log.SetLevel(c.LoggingConfig.LogLevel)
	if err != nil {
		log.Errorf("Failed to parse log_level: %s", err)
		return fmt.Errorf("Failed to parse log_level: %s", err)
	}

	return nil
}

// PluginNames returns a list of strings of the configured Plugins.
func (c *Config) PluginNames() []string {
	var name []string
	for _, plugin := range c.Plugins {
		name = append(name, plugin.Name)
	}
	return name
}

func (c *Config) addPlugin(name string, pluginConfig *plugin.Config) error {
	checker, ok := collector.Plugins[name]
	if !ok {
		return fmt.Errorf("Undefined plugin: %s", name)
	}

	rp := &plugin.RunningPlugin{
		Name:   name,
		Plugin: checker(pluginConfig.InitConfig),
		Config: pluginConfig,
	}
	c.Plugins = append(c.Plugins, rp)
	return nil
}
