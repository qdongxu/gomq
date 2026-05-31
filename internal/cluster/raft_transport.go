// raft_transport.go defines the network transport interface and an in-memory
// implementation for testing.
package cluster

import (
	"fmt"
	"sync"
)

// AppendEntriesRequest is sent by the leader to replicate log entries.
type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

// AppendEntriesResponse is the reply to AppendEntries.
type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

// RequestVoteRequest is sent by a candidate to request votes.
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteResponse is the reply to RequestVote.
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// InstallSnapshotRequest is sent by the leader to transfer a snapshot.
type InstallSnapshotRequest struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
	Done              bool   `json:"done"`
}

// InstallSnapshotResponse is the reply to InstallSnapshot.
type InstallSnapshotResponse struct {
	Term uint64 `json:"term"`
}

// RaftHandler handles incoming RPC requests.
type RaftHandler interface {
	HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse
	HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse
	HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse
}

// RaftTransport is the network layer for Raft node communication.
type RaftTransport interface {
	SendAppendEntries(peer string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	SendRequestVote(peer string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	SendInstallSnapshot(peer string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
	Start(handler RaftHandler) error
	Stop() error
	LocalAddr() string
}

// MemoryTransport is an in-memory transport for testing.
type MemoryTransport struct {
	mu       sync.RWMutex
	handlers map[string]RaftHandler
	addrs    map[string]string
}

// NewMemoryTransport creates a new in-memory transport.
func NewMemoryTransport() *MemoryTransport {
	return &MemoryTransport{
		handlers: make(map[string]RaftHandler),
		addrs:    make(map[string]string),
	}
}

// RegisterPeer registers a peer with its address and handler.
func (t *MemoryTransport) RegisterPeer(id, addr string, handler RaftHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[id] = handler
	t.addrs[id] = addr
}

// RemovePeer removes a peer from the transport.
func (t *MemoryTransport) RemovePeer(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.handlers, id)
	delete(t.addrs, id)
}

// SendAppendEntries sends an AppendEntries request to a peer.
func (t *MemoryTransport) SendAppendEntries(peer string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	t.mu.RLock()
	h, ok := t.handlers[peer]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("peer %s not found", peer)
	}
	return h.HandleAppendEntries(req), nil
}

// SendRequestVote sends a RequestVote request to a peer.
func (t *MemoryTransport) SendRequestVote(peer string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	t.mu.RLock()
	h, ok := t.handlers[peer]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("peer %s not found", peer)
	}
	return h.HandleRequestVote(req), nil
}

// SendInstallSnapshot sends an InstallSnapshot request to a peer.
func (t *MemoryTransport) SendInstallSnapshot(peer string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	t.mu.RLock()
	h, ok := t.handlers[peer]
	t.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("peer %s not found", peer)
	}
	return h.HandleInstallSnapshot(req), nil
}

// Start is a no-op for in-memory transport.
func (t *MemoryTransport) Start(handler RaftHandler) error { return nil }

// Stop is a no-op for in-memory transport.
func (t *MemoryTransport) Stop() error { return nil }

// LocalAddr returns an empty string for in-memory transport.
func (t *MemoryTransport) LocalAddr() string { return "" }
