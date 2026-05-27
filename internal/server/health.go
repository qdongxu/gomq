// health.go provides HTTP health-check and readiness endpoints.
package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// HealthResponse is the JSON body returned by /api/health.
type HealthResponse struct {
	Status    string            `json:"status"`
	Version   string            `json:"version"`
	Uptime    string            `json:"uptime"`
	Timestamp int64             `json:"timestamp"`
	Checks    map[string]Check  `json:"checks"`
}

// Check holds the result of a single health check.
type Check struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// ReadyResponse is the JSON body returned by /api/ready.
type ReadyResponse struct {
	Ready  bool              `json:"ready"`
	Checks map[string]Check  `json:"checks"`
}

// HealthHandler handles /api/health requests.
// It returns the overall node status, version, uptime, and per-component
// health checks.
func (s *Server) HealthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	checks := make(map[string]Check)

	// Listener check.
	s.mu.RLock()
	hasListener := s.listener != nil || s.tlsListener != nil
	s.mu.RUnlock()
	if hasListener {
		checks["listener"] = Check{Status: "up"}
	} else {
		checks["listener"] = Check{Status: "down", Message: "no active listener"}
	}

	// Store (etcd) check — only when a metaStore is configured.
	if s.metaStore != nil {
		// Lightweight check: attempt to load queues with a short timeout.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := s.metaStore.LoadQueues(ctx)
		cancel()
		if err != nil {
			checks["store"] = Check{Status: "down", Message: err.Error()}
		} else {
			checks["store"] = Check{Status: "up"}
		}
	} else {
		checks["store"] = Check{Status: "up", Message: "memory store (no persistence)"}
	}

	// Overall status: down if any critical check is down.
	status := "up"
	for _, c := range checks {
		if c.Status == "down" {
			status = "down"
			break
		}
	}

	resp := HealthResponse{
		Status:    status,
		Version:   s.version(),
		Uptime:    time.Since(s.StartTime()).Round(time.Second).String(),
		Timestamp: time.Now().Unix(),
		Checks:    checks,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// ReadyHandler handles /api/ready requests.
// It returns whether the node is ready to accept traffic.
func (s *Server) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	checks := make(map[string]Check)
	ready := true

	// Listener readiness.
	s.mu.RLock()
	hasListener := s.listener != nil || s.tlsListener != nil
	s.mu.RUnlock()
	if !hasListener {
		ready = false
		checks["listener"] = Check{Status: "down", Message: "no active listener"}
	} else {
		checks["listener"] = Check{Status: "up"}
	}

	// Persistence readiness (etcd only — memory store is always ready).
	if s.metaStore != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := s.metaStore.LoadQueues(ctx)
		cancel()
		if err != nil {
			ready = false
			checks["store"] = Check{Status: "down", Message: err.Error()}
		} else {
			checks["store"] = Check{Status: "up"}
		}
	} else {
		checks["store"] = Check{Status: "up", Message: "memory store"}
	}

	resp := ReadyResponse{
		Ready:  ready,
		Checks: checks,
	}

	code := http.StatusOK
	if !ready {
		code = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

// version returns the server version string.
func (s *Server) version() string {
	// Hard-coded for now; may be injected at build time later.
	return "0.1.0"
}
