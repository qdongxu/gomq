// management.go registers and serves HTTP health/readiness/pprof endpoints.
package server

import (
	"log"
	"net/http"
)

// ManagementServer wraps the HTTP mux for health/readiness/pprof.
type ManagementServer struct {
	mux *http.ServeMux
	srv *Server
}

// NewManagementServer creates a management server with routes registered.
func NewManagementServer(srv *Server) *ManagementServer {
	ms := &ManagementServer{
		mux: http.NewServeMux(),
		srv: srv,
	}
	ms.registerRoutes()
	return ms
}

func (ms *ManagementServer) registerRoutes() {
	ms.mux.HandleFunc("/api/health", ms.srv.HealthHandler)
	ms.mux.HandleFunc("/api/ready", ms.srv.ReadyHandler)
}

// EnablePprof registers /debug/pprof/* routes.
// Call only when debug logging is enabled.
func (ms *ManagementServer) EnablePprof() {
	RegisterPprof(ms.mux)
}

// ServeHTTP implements http.Handler.
func (ms *ManagementServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ms.mux.ServeHTTP(w, r)
}

// ListenAndServe starts the management HTTP server on the given address.
func (ms *ManagementServer) ListenAndServe(addr string) error {
	log.Printf("management endpoint: http://%s/api/health", addr)
	return http.ListenAndServe(addr, ms)
}
