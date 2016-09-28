# Cloudinsight Agent

Cloudinsight Agent is written in Go for collecting metrics from the system it's
running on, or from other services, and sending them to [Cloudinsight](https://cloud.oneapm.com).


## Building from source

To build Cloudinsight Agent from the source code yourself you need to have a working Go environment with [version 1.5 or greater installed](https://golang.org/doc/install).

```
$ mkdir -p $GOPATH/src/git.oneapm.me/cloud-insight
$ cd $GOPATH/src/git.oneapm.me/cloud-insight
$ git clone git@git.oneapm.me:cloud-insight/cloudinsight-agent.git
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

Run the agent in foregound:

```
$ ./bin/cloudinsight-agent
```
