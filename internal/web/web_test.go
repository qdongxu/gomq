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
	channels    []ChannelInfo
	overview    OverviewInfo
}

func (m *mockBroker) ExchangeList() []ExchangeInfo     { return m.exchanges }
func (m *mockBroker) QueueList() []QueueInfo           { return m.queues }
func (m *mockBroker) BindingList() []BindingInfo         { return m.bindings }
func (m *mockBroker) ConnectionList() []ConnectionInfo   { return m.connections }
func (m *mockBroker) ChannelList() []ChannelInfo       { return m.channels }
func (m *mockBroker) Overview() OverviewInfo             { return m.overview }

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
	if !contains(body, "Overview") {
		t.Fatal("missing Overview section in index response")
	}
}

func TestHandleExchanges(t *testing.T) {
	SetExchangesBroker(&mockBroker{
		exchanges: []ExchangeInfo{
			{Name: "amq.direct", Type: "direct",
				Durable: true, Bindings: 2,
				MessagesIn: 10, MessagesOut: 15},
			{Name: "amq.topic", Type: "topic",
				Durable: true, Bindings: 0,
				MessagesIn: 5, MessagesOut: 5},
		},
	})
	defer SetExchangesBroker(nil)

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
	if len(out) != 2 {
		t.Fatalf("unexpected len: %d", len(out))
	}
	if out[0].Name != "amq.direct" || out[0].Bindings != 2 {
		t.Fatalf("unexpected response: %+v", out[0])
	}
	if out[1].MessagesIn != 5 || out[1].MessagesOut != 5 {
		t.Fatalf("unexpected response: %+v", out[1])
	}
}

func TestHandleExchangesFilter(t *testing.T) {
	SetExchangesBroker(&mockBroker{
		exchanges: []ExchangeInfo{
			{Name: "amq.direct", Type: "direct",
				Durable: true, Bindings: 2},
			{Name: "amq.topic", Type: "topic",
				Durable: true, Bindings: 0},
		},
	})
	defer SetExchangesBroker(nil)

	srv := NewServer()
	req := httptest.NewRequest("GET",
		"/api/exchanges?type=direct", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var out []ExchangeInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 || out[0].Type != "direct" {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestHandleQueues(t *testing.T) {
	SetQueuesBroker(&mockBroker{
		queues: []QueueInfo{
			{Name: "q1", Durable: true, Messages: 3,
				Consumers: 2, Bindings: 1, Memory: 256},
			{Name: "q2", Durable: false, Messages: 0,
				Consumers: 0, Bindings: 0, Memory: 0},
		},
	})
	defer SetQueuesBroker(nil)

	srv := NewServer()
	req := httptest.NewRequest("GET", "/api/queues", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var out []QueueInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("unexpected len: %d", len(out))
	}
	if out[0].Messages != 3 || out[0].Memory != 256 {
		t.Fatalf("unexpected response: %+v", out[0])
	}
	if out[1].Durable {
		t.Fatalf("unexpected durable: %+v", out[1])
	}
}

func TestHandleQueuesFilter(t *testing.T) {
	SetQueuesBroker(&mockBroker{
		queues: []QueueInfo{
			{Name: "q1", Durable: true, Messages: 3,
				Consumers: 2, Bindings: 1, Memory: 256},
			{Name: "q2", Durable: false, Messages: 0,
				Consumers: 0, Bindings: 0, Memory: 0},
		},
	})
	defer SetQueuesBroker(nil)

	srv := NewServer()
	req := httptest.NewRequest("GET",
		"/api/queues?durable=true", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var out []QueueInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 || !out[0].Durable {
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
			{RemoteAddr: "127.0.0.1:12345", State: "open",
				Channels: 2, Heartbeat: 60},
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

func TestHandleChannels(t *testing.T) {
	SetChannelsBroker(&mockBroker{
		channels: []ChannelInfo{
			{ID: 1, Connection: "127.0.0.1:12345",
				State: "open", Consumers: 2, Unacked: 5,
				PrefetchCount: 3, PrefetchLimit: 10},
			{ID: 2, Connection: "127.0.0.1:12346",
				State: "open", Consumers: 0, Unacked: 0,
				PrefetchCount: 0, PrefetchLimit: 0},
		},
	})
	defer SetChannelsBroker(nil)

	srv := NewServer()
	req := httptest.NewRequest("GET", "/api/channels", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var out []ChannelInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 2 || out[0].ID != 1 {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestHandleChannelsFilter(t *testing.T) {
	SetChannelsBroker(&mockBroker{
		channels: []ChannelInfo{
			{ID: 1, Connection: "127.0.0.1:12345",
				State: "open", Consumers: 2, Unacked: 5,
				PrefetchCount: 3, PrefetchLimit: 10},
			{ID: 2, Connection: "127.0.0.1:12346",
				State: "open", Consumers: 0, Unacked: 0,
				PrefetchCount: 0, PrefetchLimit: 0},
		},
	})
	defer SetChannelsBroker(nil)

	srv := NewServer()
	req := httptest.NewRequest("GET",
		"/api/channels?connection=127.0.0.1:12345", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	var out []ChannelInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 || out[0].Connection != "127.0.0.1:12345" {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestHandleOverview(t *testing.T) {
	SetOverviewBroker(&mockBroker{
		overview: OverviewInfo{
			Connections: 3,
			Channels:    7,
			Exchanges:   5,
			Queues:      2,
			Consumers:   1,
			Messages:    42,
		},
	})
	defer SetOverviewBroker(nil)

	srv := NewServer()
	req := httptest.NewRequest("GET", "/api/overview", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var out OverviewInfo
	if err := json.NewDecoder(w.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Connections != 3 || out.Messages != 42 {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 &&
		containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
