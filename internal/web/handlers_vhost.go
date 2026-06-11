// handlers_vhost.go implements the VHost API for the web UI.
package web

import (
	"encoding/json"
	"net/http"
)

// VHostListInfo holds a single VHost entry for the VHost list page.
type VHostListInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Connections int    `json:"connections"`
	Queues      int    `json:"queues"`
	Exchanges   int    `json:"exchanges"`
}

// VHostBroker is the subset of broker state needed for VHost info.
type VHostBroker interface {
	VHostList() []VHostListInfo
	CreateVHost(name, description string) bool
	DeleteVHost(name string) bool
}

var vhostBroker VHostBroker

// SetVHostBroker injects the VHost data provider.
func SetVHostBroker(b VHostBroker) {
	vhostBroker = b
}

func (s *Server) handleVHosts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleVHostList(w, r)
	case http.MethodPost:
		s.handleVHostCreate(w, r)
	case http.MethodDelete:
		s.handleVHostDelete(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleVHostList(w http.ResponseWriter, r *http.Request) {
	if vhostBroker == nil {
		writeJSON(w, []VHostListInfo{})
		return
	}
	writeJSON(w, vhostBroker.VHostList())
}

func (s *Server) handleVHostCreate(w http.ResponseWriter, r *http.Request) {
	if vhostBroker == nil {
		http.Error(w, "broker not available", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}

	if !vhostBroker.CreateVHost(req.Name, req.Description) {
		http.Error(w, "VHost already exists or name invalid", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleVHostDelete(w http.ResponseWriter, r *http.Request) {
	if vhostBroker == nil {
		http.Error(w, "broker not available", http.StatusServiceUnavailable)
		return
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}

	if !vhostBroker.DeleteVHost(name) {
		http.Error(w, "VHost not found or default VHost cannot be deleted", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
