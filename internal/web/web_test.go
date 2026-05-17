// web_test.go tests the web UI handlers.
package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockBroker struct {
	exchanges   []ExchangeInfo
	queues      []QueueInfo
	bindings    []BindingInfo
	connections []ConnectionInfo
}

func (m *mockBroker) ExchangeList() []ExchangeInfo   { return m.exchanges }
func (m *mockBroker) QueueList() []QueueInfo           { return m.queues }
func (m *mockBroker) BindingList() []BindingInfo       { return m.bindings }
func (m *mockBroker) ConnectionList() []ConnectionInfo { return m.connections }

func TestHandleIndex(t *testing.T) {
	srv := NewServer()
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if w.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("content-type = %q", w.Header().Get("Content-Type"))
	}
	body := w.Body.String()
	if !contains(body, "gomq management") {
		t.Fatal("missing title in index response")
	}
	if !contains(body, "htmx.org") {
		t.Fatal("missing htmx script in index response")
	}
	if !contains(body, "Connections") {
		t.Fatal("missing Connections section in index response")
	}
}

func TestHandleExchanges(t *testing.T) {
	SetBroker(&mockBroker{
		exchanges: []ExchangeInfo{
			{Name: "amq.direct", Type: "direct"},
		},
	})
	defer SetBroker(nil)

	srv := NewServer()
	req := httptest.NewRequest("GET", "/api/exchanges", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var out []ExchangeInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 || out[0].Name != "amq.direct" {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestHandleQueues(t *testing.T) {
	SetBroker(&mockBroker{
		queues: []QueueInfo{
			{Name: "q1", Durable: true, Messages: 3, Consumers: 0},
		},
	})
	defer SetBroker(nil)

	srv := NewServer()
	req := httptest.NewRequest("GET", "/api/queues", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var out []QueueInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 || out[0].Messages != 3 {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestHandleBindings(t *testing.T) {
	SetBroker(&mockBroker{
		bindings: []BindingInfo{
			{Exchange: "ex1", Queue: "q1", RoutingKey: "rk"},
		},
	})
	defer SetBroker(nil)

	srv := NewServer()
	req := httptest.NewRequest("GET", "/api/bindings", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var out []BindingInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 || out[0].RoutingKey != "rk" {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestHandleConnections(t *testing.T) {
	SetBroker(&mockBroker{
		connections: []ConnectionInfo{
			{RemoteAddr: "127.0.0.1:12345", State: "open", Channels: 2, Heartbeat: 60},
		},
	})
	defer SetBroker(nil)

	srv := NewServer()
	req := httptest.NewRequest("GET", "/api/connections", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var out []ConnectionInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 || out[0].RemoteAddr != "127.0.0.1:12345" {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
