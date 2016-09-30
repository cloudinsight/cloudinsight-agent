package config

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/cloudinsight/cloudinsight-agent/collector"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
	"github.com/cloudinsight/cloudinsight-agent/common/plugin"
)

// VERSION sets the agent version here.
const VERSION = "0.0.1"

var (
	// DefaultConfig is the default top-level configuration.
	DefaultConfig = Config{
		GlobalConfig: DefaultGlobalConfig,
	}

	// DefaultGlobalConfig is the default global configuration.
	DefaultGlobalConfig = GlobalConfig{
		CiURL:      "https://dc-cloud.oneapm.com",
		BindHost:   "127.0.0.1",
		ListenPort: 10010,
		StatsdPort: 8251,
	}
)

// NewConfig creates a new instance of Config.
func NewConfig(confPath string) (*Config, error) {
	c := &Config{}
	*c = DefaultConfig

	err := c.LoadConfig(confPath)
	if err != nil {
		return nil, fmt.Errorf("Failed to load the config file: %s", err)
	}

	if c.GlobalConfig.LicenseKey == "" {
		return nil, fmt.Errorf("LicenseKey must be specified in the config file.")
	}

	return c, nil
}

// Config represents cloudinsight-agent's configuration file.
type Config struct {
	GlobalConfig  GlobalConfig  `toml:"global"`
	LoggingConfig LoggingConfig `toml:"logging"`
	Plugins       []*plugin.RunningPlugin
}

// GlobalConfig XXX
type GlobalConfig struct {
	CiURL      string `toml:"ci_url"`
	LicenseKey string `toml:"license_key"`
	Hostname   string `toml:"hostname"`
	Tags       string `toml:"tags"`
	BindHost   string `toml:"bind_host"`
	ListenPort int    `toml:"listen_port"`
	StatsdPort int    `toml:"statsd_port"`
}

// LoggingConfig XXX
type LoggingConfig struct {
	LogLevel string `toml:"log_level"`
	LogFile  string `toml:"log_file"`
}

// Try to find a default config file at these locations (in order):
//   1. $CWD/cloudinsight-agent.conf
//   2. /etc/cloudinsight-agent/cloudinsight-agent.conf
//   3. $HOME/.cloudinsight-agent/cloudinsight-agent.conf
//
func getDefaultConfigPath() (string, error) {
	file := "cloudinsight-agent.conf"
	etcfile := "/etc/cloudinsight-agent/cloudinsight-agent.conf"
	homefile := os.ExpandEnv("$HOME/.cloudinsight-agent/cloudinsight-agent.conf")
	for _, path := range []string{file, etcfile, homefile} {
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
		m, _ := filepath.Glob(filepath.Join(root, "collector/conf.d", pattern))
		files = append(files, m...)
	}

	for _, file := range files {
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

// PluginNames returns a list of strings of the configured Plugins.
func (c *Config) PluginNames() []string {
	var name []string
	for _, plugin := range c.Plugins {
		name = append(name, plugin.Name)
	}
	return name
}

// GetForwarderAddr gets the address that Forwarder listening to.
func (c *Config) GetForwarderAddr() string {
	return fmt.Sprintf("%s:%d", c.GlobalConfig.BindHost, c.GlobalConfig.ListenPort)
}

// GetForwarderAddrWithScheme gets the address of Forwarder with scheme prefix.
func (c *Config) GetForwarderAddrWithScheme() string {
	return fmt.Sprintf("http://%s", c.GetForwarderAddr())
}

// GetStatsdAddr gets the address that Statsd listening to.
func (c *Config) GetStatsdAddr() string {
	return fmt.Sprintf("%s:%d", c.GlobalConfig.BindHost, c.GlobalConfig.StatsdPort)
}

// GetHostname gets the hostname from os itself if not set in the agent configuration.
func (c *Config) GetHostname() string {
	hostname := c.GlobalConfig.Hostname
	if hostname != "" {
		return hostname
	}

	var err error
	hostname, err = os.Hostname()
	if err != nil {
		log.Error(err)
	}
	return hostname
}

//InitializeLogging initializes logging level and output according to the agent configuration.
func (c *Config) InitializeLogging() error {
	log.Infoln("Initialize log...")
	err := log.SetLevel(c.LoggingConfig.LogLevel)
	if err != nil {
		return fmt.Errorf("Failed to parse log_level: %s", err)
	}

	logFile := c.LoggingConfig.LogFile

	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	log.SetOutput(f)

	return nil
}
