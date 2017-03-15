# Cloudinsight Agent

[![Build Status](https://travis-ci.org/cloudinsight/cloudinsight-agent.svg?branch=master)](https://travis-ci.org/cloudinsight/cloudinsight-agent)
[![Go Report Card](https://goreportcard.com/badge/github.com/cloudinsight/cloudinsight-agent)](https://goreportcard.com/report/github.com/cloudinsight/cloudinsight-agent)

Cloudinsight 探针可以收集它所在操作系统的各种指标，然后发送到 [Cloudinsight](https://cloud.oneapm.com) 后端服务，探针由 Go 语言实现。

## 源代码编译

为了从源代码编译 Cloudinsight 探针，你需要准备一个 Go 语言环境，版本需要 [>= 1.7](https://golang.org/doc/install)。

```
$ mkdir -p $GOPATH/src/github.com/cloudinsight
$ cd $GOPATH/src/github.com/cloudinsight
$ git clone https://github.com/cloudinsight/cloudinsight-agent
$ cd cloudinsight-agent
$ make build
```

## 使用方法

首先需要设置 license key，你可以在这里找到你的 license key，[https://cloud.oneapm.com/#/settings](https://cloud.oneapm.com/#/settings).

```
$ cp cloudinsight-agent.conf.example cloudinsight-agent.conf
$ vi cloudinsight-agent.conf
...
license_key = "*********************"
```

在前台运行探针：

```
$ ./bin/cloudinsight-agent
```

更多用法, 见:

```
$ ./bin/cloudinsight-agent --help
```

## 相关的资源

Cloudinsight 探针深受以下项目的影响：

- [ddagent](https://github.com/datadog/dd-agent)
- [telegraf](https://github.com/influxdata/telegraf)
- [prometheus](https://github.com/prometheus/prometheus)
- [mackerel](https://github.com/mackerelio/mackerel-agent)
