package forwarder

import (
	"net"
	"net/http"
	"time"

	"github.com/startover/cloudinsight-agent/common/api"
	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/log"
)

// NewForwarder XXX
func NewForwarder(conf *config.Config) *Forwarder {
	api := api.NewAPI(conf.GlobalConfig.CiURL, conf.GlobalConfig.LicenseKey, 10*time.Second)
	return &Forwarder{
		api:  api,
		conf: conf,
	}
}

// Forwarder XXX
type Forwarder struct {
	api  *api.API
	conf *config.Config
}

// Run XXX
func (f *Forwarder) Run(shutdown chan struct{}) error {
	http.HandleFunc("/infrastructure/metrics", func(w http.ResponseWriter, r *http.Request) {
		err := f.api.Post(f.api.GetURL("metrics"), r.Body)
		if err != nil {
			log.Errorf("Error occured when posting Payload. %s", err)
		}
	})

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

	l, err := net.Listen("tcp", f.conf.GetForwarderAddr())
	if err != nil {
		return err
	}

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
