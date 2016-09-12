package collector

import "github.com/startover/cloudinsight-agent/common/plugin"

// Checker XXX
type Checker func(conf plugin.InitConfig) plugin.Plugin

// Plugins XXX
var Plugins = map[string]Checker{}

// Add XXX
func Add(name string, checker Checker) {
	Plugins[name] = checker
}
