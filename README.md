# Cloudinsight Agent  [![Build Status](https://travis-ci.org/cloudinsight/cloudinsight-agent.svg)][travis]

Cloudinsight Agent is written in Go for collecting metrics from the system it's
running on, or from other services, and sending them to [Cloudinsight](https://cloud.oneapm.com).

## Building from source

To build Cloudinsight Agent from the source code yourself you need to have a working Go environment with [version 1.5 or greater installed](https://golang.org/doc/install).

```
$ mkdir -p $GOPATH/src/github.com/cloudinsight
$ cd $GOPATH/src/github.com/cloudinsight
$ git clone https://github.com/cloudinsight/cloudinsight-agent
$ cd cloudinsight-agent
$ make build
```

## Usage

First you need to set a license key, which can be found at [https://cloud.oneapm.com/#/settings](https://cloud.oneapm.com/#/settings).

```
$ cp cloudinsight-agent.conf.example cloudinsight-agent.conf
$ vi cloudinsight-agent.conf
...
license_key = "*********************"
```

Run the agent in foreground:

```
$ ./bin/cloudinsight-agent
```

## Related works

I have been influenced by the following great works:

- [ddagent](https://github.com/datadog/dd-agent)
- [telegraf](https://github.com/influxdata/telegraf)
- [prometheus](https://github.com/prometheus/prometheus)
- [mackerel](https://github.com/mackerelio/mackerel-agent)
