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

//go:embed static/i18n/en.json
var i18nEN []byte

//go:embed static/i18n/zh.json
var i18nZH []byte

//go:embed static/i18n/ja.json
var i18nJA []byte

// Server wraps HTTP handlers for the management UI.
type Server struct {
	mux        *http.ServeMux
	store      *SessionStore
	auth       *AuthMiddleware
	csrf       *CSRFMiddleware
	i18n       *I18n
}


// NewServer creates a web UI server with routes registered.
func NewServer(authCfg AuthConfig) *Server {
	store := NewSessionStore()
	auth := NewAuthMiddleware(store, authCfg)
	csrf := NewCSRFMiddleware(store)
	i18n := NewI18n()
	_ = i18n.Load("en", i18nEN)
	_ = i18n.Load("zh", i18nZH)
	_ = i18n.Load("ja", i18nJA)
	s := &Server{
		mux:   http.NewServeMux(),
		store: store,
		auth:  auth,
		csrf:  csrf,
		i18n:  i18n,
	}
	s.registerRoutes()
	return s
}

// NewServerWithWebSocket creates a server with a WebSocket hub.
func NewServerWithWebSocket(hub *Hub, authCfg AuthConfig) *Server {
	store := NewSessionStore()
	auth := NewAuthMiddleware(store, authCfg)
	csrf := NewCSRFMiddleware(store)
	i18n := NewI18n()
	_ = i18n.Load("en", i18nEN)
	_ = i18n.Load("zh", i18nZH)
	_ = i18n.Load("ja", i18nJA)
	s := &Server{
		mux:   http.NewServeMux(),
		store: store,
		auth:  auth,
		csrf:  csrf,
		i18n:  i18n,
	}
	s.registerRoutes()
	s.registerWebSocket(hub)
	return s
}


func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/", s.wrap(s.handleIndex))
	s.mux.HandleFunc("/login", s.wrap(s.handleLoginPage))
	s.mux.HandleFunc("/api/login", s.wrap(s.auth.HandleLogin))
	s.mux.HandleFunc("/api/logout", s.wrap(s.auth.HandleLogout))
	s.mux.HandleFunc("/api/overview", s.wrap(s.handleOverview))
	s.mux.HandleFunc("/api/admin", s.wrap(s.handleAdmin))
	s.mux.HandleFunc("/api/exchanges", s.wrap(s.handleExchanges))
	s.mux.HandleFunc("/api/queues", s.wrap(s.handleQueues))
	s.mux.HandleFunc("/api/bindings", s.wrap(s.handleBindings))
	s.mux.HandleFunc("/api/connections", s.wrap(s.handleConnections))
	s.mux.HandleFunc("/api/channels", s.wrap(s.handleChannels))
	s.mux.HandleFunc("/api/messages", s.wrap(s.handleMessages))
	s.mux.HandleFunc("/api/cluster", s.wrap(s.handleCluster))
	s.mux.HandleFunc("/api/vhosts", s.wrap(s.handleVHosts))
	s.mux.HandleFunc("/api/lang", s.wrap(s.handleSetLang))
}

// wrap applies auth + csrf middleware.
func (s *Server) wrap(h http.HandlerFunc) http.HandlerFunc {
	return s.auth.Require(s.csrf.Protect(h))
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
	lang := s.i18n.DetectLang(r)
	fn := template.FuncMap{
		"i18n": func(key string) string {
			return s.i18n.T(lang, key)
		},
	}
	tmpl, err := template.New("index").Funcs(fn).Parse(indexHTML)
	if err != nil {
		http.Error(w, "template error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	data := map[string]interface{}{
		"CSRFMeta": template.HTML(CSRFMetaTag(s.csrf.CSRFTokenForRequest(r))),
		"Lang":     lang,
		"Langs":    s.i18n.Supported(),
	}
	_ = tmpl.Execute(w, data)
}

func (s *Server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = loginTemplate.Execute(w, nil)
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

//go:embed templates/login.html
var loginHTML string

var loginTemplate = template.Must(template.New("login").Parse(loginHTML))

