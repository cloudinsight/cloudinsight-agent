package forwarder

import (
	"net"
	"net/http"
	"time"

	"github.com/cloudinsight/cloudinsight-agent/common/api"
	"github.com/cloudinsight/cloudinsight-agent/common/config"
	"github.com/cloudinsight/cloudinsight-agent/common/log"
)

// NewForwarder creates a new instance of Forwarder.
func NewForwarder(conf *config.Config) *Forwarder {
	api := api.NewAPI(conf.GlobalConfig.CiURL, conf.GlobalConfig.LicenseKey, 10*time.Second, conf.GlobalConfig.Proxy)
	return &Forwarder{
		api:  api,
		conf: conf,
	}
}

// Forwarder sends the metrics to Cloudinsight data center, which is collected by Collector and Statsd.
type Forwarder struct {
	api  *api.API
	conf *config.Config
}

func (f *Forwarder) metricHandler(w http.ResponseWriter, r *http.Request) {
	err := f.api.Post(f.api.GetURL("metrics"), r.Body)
	if err != nil {
		log.Errorf("Error occurred when posting Payload. %s", err)
	}
}

// Run runs a http server listening to 10010 as default.
func (f *Forwarder) Run(shutdown chan struct{}) error {
	http.HandleFunc("/infrastructure/metrics", f.metricHandler)

	http.HandleFunc("/infrastructure/series", func(w http.ResponseWriter, r *http.Request) {
		// TODO
	})

	http.HandleFunc("/infrastructure/service_checks", func(w http.ResponseWriter, r *http.Request) {
		// TODO
	})

	s := &http.Server{
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	addr := f.conf.GetForwarderAddr()

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	log.Infoln("Forwarder listening on:", addr)

	go func() {
		if err := s.Serve(l); err != nil {
			log.Fatal(err)
		}
	}()

	select {
	case <-shutdown:
		log.Infof("Forwarder server thread exit")
		if err := l.Close(); err != nil {
			return err
		}
	}

	return nil
}
