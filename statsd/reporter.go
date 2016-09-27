package statsd

import (
	"time"

	"github.com/startover/cloudinsight-agent/common/api"
	"github.com/startover/cloudinsight-agent/common/config"
	"github.com/startover/cloudinsight-agent/common/emitter"
	"github.com/startover/cloudinsight-agent/common/log"
)

// Reporter XXX
type Reporter struct {
	*emitter.Emitter

	api  *api.API
	conf *config.Config
}

// NewReporter XXX
func NewReporter(conf *config.Config) *Reporter {
	emitter := emitter.NewEmitter("Statsd")
	api := api.NewAPI(conf.GetForwarderAddrWithScheme(), conf.GlobalConfig.LicenseKey, 5*time.Second)

	r := &Reporter{
		Emitter: emitter,
		api:     api,
		conf:    conf,
	}
	r.Emitter.Parent = r

	return r
}

// Post XXX
func (r *Reporter) Post(metrics []interface{}) error {
	start := time.Now()
	payload := Payload{}
	payload.Series = metrics

	log.Infoln("Submitting metrics:", payload)

	err := r.api.SubmitMetrics(&payload)
	elapsed := time.Since(start)
	if err == nil {
		log.Infof("Write batch of %d metrics in %s\n",
			len(metrics), elapsed)
	}
	return err
}
