// handlers_admin.go implements the Admin API for the web UI.
package web

import "net/http"

// VHostInfo holds virtual-host data for the UI.
type VHostInfo struct {
	Name      string `json:"name"`
	Exchanges int    `json:"exchanges"`
	Queues    int    `json:"queues"`
}

// UserInfo holds user data for the UI.
type UserInfo struct {
	Username string `json:"username"`
}

// AdminInfo holds server admin data for the UI.
type AdminInfo struct {
	Version string      `json:"version"`
	Uptime  string      `json:"uptime"`
	VHosts  []VHostInfo `json:"vhosts"`
	Users   []UserInfo  `json:"users"`
}

// AdminBroker is the subset of broker state needed for admin.
type AdminBroker interface {
	Admin() AdminInfo
}

var adminBroker AdminBroker

// SetAdminBroker injects the admin data provider.
func SetAdminBroker(b AdminBroker) {
	adminBroker = b
}

func (s *Server) handleAdmin(
	w http.ResponseWriter,
	r *http.Request,
) {
	if adminBroker == nil {
		writeJSON(w, AdminInfo{})
		return
	}
	writeJSON(w, adminBroker.Admin())
}
