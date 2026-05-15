// web.go provides the HTTP management UI for gomq.
package web

import (
	"encoding/json"
	"html/template"
	"net/http"
)

// Server wraps HTTP handlers for the management UI.
type Server struct {
	mux *http.ServeMux
}

// NewServer creates a web UI server with routes registered.
func NewServer() *Server {
	s := &Server{mux: http.NewServeMux()}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/", s.handleIndex)
	s.mux.HandleFunc("/api/exchanges", s.handleExchanges)
	s.mux.HandleFunc("/api/queues", s.handleQueues)
	s.mux.HandleFunc("/api/bindings", s.handleBindings)
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Broker is the subset of broker state exposed to the web UI.
type Broker interface {
	ExchangeList() []ExchangeInfo
	QueueList() []QueueInfo
	BindingList() []BindingInfo
}

// ExchangeInfo holds exchange data for the UI.
type ExchangeInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// QueueInfo holds queue data for the UI.
type QueueInfo struct {
	Name       string `json:"name"`
	Durable    bool   `json:"durable"`
	Messages   int    `json:"messages"`
	Consumers  int    `json:"consumers"`
}

// BindingInfo holds binding data for the UI.
type BindingInfo struct {
	Exchange   string `json:"exchange"`
	Queue      string `json:"queue"`
	RoutingKey string `json:"routing_key"`
}

var broker Broker

// SetBroker injects the broker state provider.
func SetBroker(b Broker) {
	broker = b
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = indexTemplate.Execute(w, nil)
}

func (s *Server) handleExchanges(w http.ResponseWriter, r *http.Request) {
	if broker == nil {
		writeJSON(w, []ExchangeInfo{})
		return
	}
	writeJSON(w, broker.ExchangeList())
}

func (s *Server) handleQueues(w http.ResponseWriter, r *http.Request) {
	if broker == nil {
		writeJSON(w, []QueueInfo{})
		return
	}
	writeJSON(w, broker.QueueList())
}

func (s *Server) handleBindings(w http.ResponseWriter, r *http.Request) {
	if broker == nil {
		writeJSON(w, []BindingInfo{})
		return
	}
	writeJSON(w, broker.BindingList())
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

var indexTemplate = template.Must(template.New("index").Parse(`<!DOCTYPE html>
<html>
<head>
	<meta charset="UTF-8">
	<title>gomq management</title>
	<script src="https://unpkg.com/htmx.org@1.9.12"></script>
	<style>
		body { font-family: system-ui, sans-serif; margin: 2rem; }
		h1 { font-size: 1.5rem; }
		h2 { font-size: 1.2rem; margin-top: 1.5rem; }
		table { border-collapse: collapse; width: 100%; margin-top: .5rem; }
		th, td { border: 1px solid #ddd; padding: .5rem; text-align: left; }
		th { background: #f5f5f5; }
		tr:nth-child(even) { background: #fafafa; }
	</style>
</head>
<body>
	<h1>🔧 gomq management</h1>

	<h2>Exchanges</h2>
	<table hx-get="/api/exchanges" hx-trigger="load" hx-target="#exchanges">
		<thead><tr><th>Name</th><th>Type</th></tr></thead>
		<tbody id="exchanges"><td colspan="2">Loading…</td></tbody>
	</table>

	<h2>Queues</h2>
	<table hx-get="/api/queues" hx-trigger="load" hx-target="#queues">
		<thead><tr><th>Name</th><th>Durable</th><th>Messages</th><th>Consumers</th></tr></thead>
		<tbody id="queues"><td colspan="4">Loading…</td></tbody>
	</table>

	<h2>Bindings</h2>
	<table hx-get="/api/bindings" hx-trigger="load" hx-target="#bindings">
		<thead><tr><th>Exchange</th><th>Queue</th><th>Routing Key</th></tr></thead>
		<tbody id="bindings"><td colspan="3">Loading…</td></tbody>
	</table>
</body>
</html>
`))
