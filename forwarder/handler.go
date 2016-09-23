package forwarder

import (
	"fmt"
	"time"

	"github.com/startover/cloudinsight-agent/common/api"
	"github.com/startover/cloudinsight-agent/common/config"
)

// Handler XXX
type Handler struct {
	api *api.API
}

// NewHandler XXX
func NewHandler(config *config.Config) (*Handler, error) {
	if config.GlobalConfig.LicenseKey == "" {
		return nil, fmt.Errorf("LicenseKey is required for cloudinsight. You can find it at https://cloud.oneapm.com/#/settings")
	}

	api := api.NewAPI(config.GlobalConfig.CiURL, config.GlobalConfig.LicenseKey, 10*time.Second)
	c := &Handler{
		api: api,
	}
	return c, nil
}

// Post XXX
func (h *Handler) Post(payload *api.Payload) error {
	err := h.api.Post(payload)
	return err
}
