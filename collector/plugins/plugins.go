package plugins

import (
	// registry all plugins
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/apache"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/docker"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/haproxy"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/memcached"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/mongodb"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/mysql"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/nginx"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/phpfpm"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/postgres"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/redis"
	_ "github.com/cloudinsight/cloudinsight-agent/collector/plugins/system"
)
