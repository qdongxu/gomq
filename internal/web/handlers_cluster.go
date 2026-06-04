// handlers_cluster.go implements the Cluster API for the web UI.
package web

import (
	"encoding/json"
	"net/http"
)

// ClusterNodeInfo holds a single node's data for the UI.
type ClusterNodeInfo struct {
	ID        string `json:"id"`
	Addr      string `json:"addr"`
	Status    string `json:"status"`
	Role      string `json:"role"`
	Uptime    string `json:"uptime"`
	Conns     int    `json:"conns"`
	MsgIn     int64  `json:"msg_in"`
	MsgOut    int64  `json:"msg_out"`
	LogIndex  uint64 `json:"log_index"`
	LogTerm   uint64 `json:"log_term"`
	Heartbeat string `json:"heartbeat_latency"`
}

// ClusterBroker is the subset of broker state needed for cluster
// info.
type ClusterBroker interface {
	ClusterList() []ClusterNodeInfo
}

var clusterBroker ClusterBroker

// SetClusterBroker injects the cluster data provider.
func SetClusterBroker(b ClusterBroker) {
	clusterBroker = b
}

func (s *Server) handleCluster(w http.ResponseWriter,
	r *http.Request) {
	if clusterBroker == nil {
		writeJSON(w, []ClusterNodeInfo{})
		return
	}
	writeJSON(w, clusterBroker.ClusterList())
}

// clusterNodeJSONResponse wraps the slice for tests.
func clusterNodeJSONResponse(nodes []ClusterNodeInfo) []byte {
	b, _ := json.Marshal(nodes)
	return b
}
