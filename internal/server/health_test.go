// health_test.go tests the health-check and readiness handlers.
package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthHandler(t *testing.T) {
	srv := NewServerWithStore(nil)
	_ = srv.Listen("127.0.0.1:0")

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rec := httptest.NewRecorder()

	srv.HealthHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp HealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal health response: %v", err)
	}

	if resp.Status != "up" {
		t.Fatalf("expected status up, got %s", resp.Status)
	}
	if resp.Version != "0.1.0" {
		t.Fatalf("expected version 0.1.0, got %s", resp.Version)
	}
	if resp.Checks["listener"].Status != "up" {
		t.Fatalf("expected listener up, got %s", resp.Checks["listener"].Status)
	}
	if resp.Checks["store"].Status != "up" {
		t.Fatalf("expected store up, got %s", resp.Checks["store"].Status)
	}
	if resp.Checks["store"].Message != "memory store (no persistence)" {
		t.Fatalf("unexpected store message: %s", resp.Checks["store"].Message)
	}

	// Ensure uptime and timestamp are populated.
	if resp.Uptime == "" {
		t.Fatal("expected non-empty uptime")
	}
	if resp.Timestamp == 0 {
		t.Fatal("expected non-zero timestamp")
	}
}

func TestHealthHandlerMethodNotAllowed(t *testing.T) {
	srv := NewServerWithStore(nil)
	req := httptest.NewRequest(http.MethodPost, "/api/health", nil)
	rec := httptest.NewRecorder()

	srv.HealthHandler(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", rec.Code)
	}
}

func TestReadyHandler(t *testing.T) {
	srv := NewServerWithStore(nil)
	_ = srv.Listen("127.0.0.1:0")

	req := httptest.NewRequest(http.MethodGet, "/api/ready", nil)
	rec := httptest.NewRecorder()

	srv.ReadyHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var resp ReadyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal ready response: %v", err)
	}

	if !resp.Ready {
		t.Fatal("expected ready=true")
	}
	if resp.Checks["listener"].Status != "up" {
		t.Fatalf("expected listener up, got %s", resp.Checks["listener"].Status)
	}
}

func TestReadyHandlerNoListener(t *testing.T) {
	// Server without listener should report not ready.
	srv := NewServerWithStore(nil)
	// Do not call Listen — listener is nil.

	req := httptest.NewRequest(http.MethodGet, "/api/ready", nil)
	rec := httptest.NewRecorder()

	srv.ReadyHandler(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rec.Code)
	}

	var resp ReadyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal ready response: %v", err)
	}

	if resp.Ready {
		t.Fatal("expected ready=false when no listener")
	}
	if resp.Checks["listener"].Status != "down" {
		t.Fatalf("expected listener down, got %s", resp.Checks["listener"].Status)
	}
}

func TestReadyHandlerMethodNotAllowed(t *testing.T) {
	srv := NewServerWithStore(nil)
	req := httptest.NewRequest(http.MethodPost, "/api/ready", nil)
	rec := httptest.NewRecorder()

	srv.ReadyHandler(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", rec.Code)
	}
}

func TestVersion(t *testing.T) {
	srv := NewServerWithStore(nil)
	v := srv.version()
	if v != "0.1.0" {
		t.Fatalf("expected version 0.1.0, got %s", v)
	}
}

func TestManagementServer(t *testing.T) {
	srv := NewServerWithStore(nil)
	_ = srv.Listen("127.0.0.1:0")
	mgmt := NewManagementServer(srv)

	// Test /api/health route.
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rec := httptest.NewRecorder()
	mgmt.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 for /api/health, got %d", rec.Code)
	}

	var health HealthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &health); err != nil {
		t.Fatalf("unmarshal health: %v", err)
	}
	if health.Status != "up" {
		t.Fatalf("expected status up, got %s", health.Status)
	}

	// Test /api/ready route.
	req = httptest.NewRequest(http.MethodGet, "/api/ready", nil)
	rec = httptest.NewRecorder()
	mgmt.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 for /api/ready, got %d", rec.Code)
	}

	var ready ReadyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &ready); err != nil {
		t.Fatalf("unmarshal ready: %v", err)
	}
	if !ready.Ready {
		t.Fatal("expected ready=true")
	}
}

func TestManagementServerPprof(t *testing.T) {
	srv := NewServerWithStore(nil)
	mgmt := NewManagementServer(srv)
	mgmt.EnablePprof()

	// Pprof index page should be reachable.
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	mgmt.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 for /debug/pprof/, got %d", rec.Code)
	}

	// Heap profile should be reachable.
	req = httptest.NewRequest(http.MethodGet, "/debug/pprof/heap", nil)
	rec = httptest.NewRecorder()
	mgmt.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 for /debug/pprof/heap, got %d", rec.Code)
	}
}

func TestStartTime(t *testing.T) {
	before := time.Now()
	srv := NewServerWithStore(nil)
	after := time.Now()

	st := srv.StartTime()
	if st.Before(before) || st.After(after) {
		t.Fatalf("start time %v not in expected range [%v, %v]", st, before, after)
	}
}
