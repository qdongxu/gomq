// web.go provides the HTTP management UI for gomq.
package web

import (
	_ "embed"
	"encoding/json"
	"html/template"
	"net/http"
)

//go:embed templates/index.html
var indexHTML string

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

// NewServerWithWebSocket creates a server with a WebSocket hub.
func NewServerWithWebSocket(hub *Hub) *Server {
	s := &Server{mux: http.NewServeMux()}
	s.registerRoutes()
	s.registerWebSocket(hub)
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/", s.handleIndex)
	s.mux.HandleFunc("/api/overview", s.handleOverview)
	s.mux.HandleFunc("/api/admin", s.handleAdmin)
	s.mux.HandleFunc("/api/exchanges", s.handleExchanges)
	s.mux.HandleFunc("/api/queues", s.handleQueues)
	s.mux.HandleFunc("/api/bindings", s.handleBindings)
	s.mux.HandleFunc("/api/connections", s.handleConnections)
	s.mux.HandleFunc("/api/channels", s.handleChannels)
	s.mux.HandleFunc("/api/messages", s.handleMessages)
}

func (s *Server) registerWebSocket(hub *Hub) {
	s.mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ServeWebSocket(hub, w, r)
	})
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// ConnectionInfo holds connection data for the UI.
type ConnectionInfo struct {
	RemoteAddr string `json:"remote_addr"`
	State      string `json:"state"`
	Channels   int    `json:"channels"`
	Heartbeat  int    `json:"heartbeat"`
}

// MessageInfo holds message data for the UI.
type MessageInfo struct {
	DeliveryTag uint64                 `json:"delivery_tag"`
	Payload     string                 `json:"payload"`
	Headers     map[string]interface{} `json:"headers"`
	Timestamp   string                 `json:"timestamp"`
	Exchange    string                 `json:"exchange"`
	RoutingKey  string                 `json:"routing_key"`
}

// Broker is the subset of broker state exposed to the web UI.
type Broker interface {
	ExchangeList() []ExchangeInfo
	QueueList() []QueueInfo
	BindingList() []BindingInfo
	ConnectionList() []ConnectionInfo
	ChannelList() []ChannelInfo
	MessageList(queueName string, limit, offset int) []MessageInfo
}

// ExchangeInfo holds exchange data for the UI.
type ExchangeInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Durable     bool   `json:"durable"`
	Bindings    int    `json:"bindings"`
	MessagesIn  int64  `json:"messages_in"`
	MessagesOut int64  `json:"messages_out"`
}

// QueueInfo holds queue data for the UI.
type QueueInfo struct {
	Name      string `json:"name"`
	Durable   bool   `json:"durable"`
	Messages  int    `json:"messages"`
	Consumers int    `json:"consumers"`
	Bindings  int    `json:"bindings"`
	Memory    int64  `json:"memory"`
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

func (s *Server) handleConnections(w http.ResponseWriter, r *http.Request) {
	if broker == nil {
		writeJSON(w, []ConnectionInfo{})
		return
	}
	writeJSON(w, broker.ConnectionList())
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

var indexTemplate = template.Must(template.New("index").Parse(indexHTML))
