package forwarder

import (
	"encoding/json"
	"net"
	"net/http"
	"time"

	_ "github.com/startover/cloudinsight-agent/collector/plugins"
	"github.com/startover/cloudinsight-agent/common/api"
	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/log"
)

func Start(shutdown chan struct{}, conf *config.Config) {
	handler, err := NewHandler(conf)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/infrastructure/metrics", func(w http.ResponseWriter, r *http.Request) {
		var p api.Payload
		err := json.NewDecoder(r.Body).Decode(&p)
		if err != nil {
			log.Errorf("Error occured when decoding Payload. %s", err)
			return
		}
		// log.Infoln(p)

		err = handler.Post(&p)
		if err != nil {
			log.Error(err)
		}
	})

	s := &http.Server{
		Addr:           ":9999",
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	l, e := net.Listen("tcp", ":9999")
	if e != nil {
		log.Error(e)
	}

	go func() {
		if err := s.Serve(l); err != nil {
			log.Fatal(err)
		}
	}()

	select {
	case <-shutdown:
		log.Infof("Server thread exit")
		if err := l.Close(); err != nil {
			log.Fatal(err)
		}
		return
	}
}
