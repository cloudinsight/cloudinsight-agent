package plugins

import (
	// registry all plugins
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/apache"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/haproxy"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/nginx"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/system"
)
