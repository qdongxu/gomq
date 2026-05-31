// raft_rpc.go implements RaftTransport over HTTP/JSON.
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// HTTPTransport implements RaftTransport over HTTP.
type HTTPTransport struct {
	addr    string
	server  *http.Server
	handler RaftHandler
	client  *http.Client
	mu      sync.RWMutex
	running bool
}

// NewHTTPTransport creates a new HTTP transport.
func NewHTTPTransport(addr string) *HTTPTransport {
	return &HTTPTransport{
		addr: addr,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// Start starts the HTTP server.
func (t *HTTPTransport) Start(handler RaftHandler) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running {
		return fmt.Errorf("already running")
	}
	t.handler = handler
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/append", t.handleAppendEntries)
	mux.HandleFunc("/raft/vote", t.handleRequestVote)
	mux.HandleFunc("/raft/snapshot", t.handleInstallSnapshot)
	t.server = &http.Server{
		Addr:    t.addr,
		Handler: mux,
	}
	t.running = true
	go func() {
		_ = t.server.ListenAndServe()
	}()
	return nil
}

// Stop stops the HTTP server.
func (t *HTTPTransport) Stop() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.running {
		return nil
	}
	t.running = false
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return t.server.Shutdown(ctx)
}

// LocalAddr returns the local address.
func (t *HTTPTransport) LocalAddr() string { return t.addr }

func (t *HTTPTransport) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	t.mu.RLock()
	h := t.handler
	t.mu.RUnlock()
	if h == nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	resp := h.HandleAppendEntries(&req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (t *HTTPTransport) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var req RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	t.mu.RLock()
	h := t.handler
	t.mu.RUnlock()
	if h == nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	resp := h.HandleRequestVote(&req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (t *HTTPTransport) handleInstallSnapshot(w http.ResponseWriter, r *http.Request) {
	var req InstallSnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	t.mu.RLock()
	h := t.handler
	t.mu.RUnlock()
	if h == nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	resp := h.HandleInstallSnapshot(&req)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// SendAppendEntries sends an AppendEntries request via HTTP.
func (t *HTTPTransport) SendAppendEntries(peer string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	resp := &AppendEntriesResponse{}
	if err := t.post(peer, "/raft/append", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SendRequestVote sends a RequestVote request via HTTP.
func (t *HTTPTransport) SendRequestVote(peer string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	resp := &RequestVoteResponse{}
	if err := t.post(peer, "/raft/vote", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SendInstallSnapshot sends an InstallSnapshot request via HTTP.
func (t *HTTPTransport) SendInstallSnapshot(peer string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	resp := &InstallSnapshotResponse{}
	if err := t.post(peer, "/raft/snapshot", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *HTTPTransport) post(peer, path string, reqBody, respBody interface{}) error {
	body, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	url := "http://" + peer + path
	r, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("http %d", r.StatusCode)
	}
	return json.NewDecoder(r.Body).Decode(respBody)
}
