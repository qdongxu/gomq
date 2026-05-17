// handlers_overview.go implements the Overview API for the web UI.
package web

import (
	"net/http"
)

// OverviewInfo holds node-level summary data for the UI.
type OverviewInfo struct {
	Connections int    `json:"connections"`
	Channels    int    `json:"channels"`
	Exchanges   int    `json:"exchanges"`
	Queues      int    `json:"queues"`
	Consumers   int    `json:"consumers"`
	Messages    int    `json:"messages"`
}

// OverviewBroker is the subset of broker state needed for overview.
type OverviewBroker interface {
	Overview() OverviewInfo
}

var overviewBroker OverviewBroker

// SetOverviewBroker injects the overview data provider.
func SetOverviewBroker(b OverviewBroker) {
	overviewBroker = b
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	if overviewBroker == nil {
		writeJSON(w, OverviewInfo{})
		return
	}
	writeJSON(w, overviewBroker.Overview())
}
