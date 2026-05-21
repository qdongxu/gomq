// quorum_manager.go manages quorum queues and replica assignment.
package queue

import (
	"fmt"
	"sync"

	"github.com/qdongxu/gomq/internal/cluster"
)

// QuorumQueueManager tracks all quorum queues.
type QuorumQueueManager struct {
	queues map[string]*QuorumQueue
	peers  []string // cluster node addresses
	mu     sync.RWMutex
}

// NewQuorumQueueManager creates a manager with the given peer set.
func NewQuorumQueueManager(peers []string) *QuorumQueueManager {
	return &QuorumQueueManager{
		queues: make(map[string]*QuorumQueue),
		peers:  peers,
	}
}

// Create creates a new quorum queue assigned to the local node as
// leader.
func (m *QuorumQueueManager) Create(
	name string,
	localNodeID string,
) (*QuorumQueue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.queues[name]; ok {
		return nil, fmt.Errorf("quorum queue %q exists", name)
	}

	raft := cluster.NewRaftNode(localNodeID, m.peers)
	// For a single-node "cluster", auto-promote to leader.
	if len(m.peers) == 0 {
		raft.BecomeLeader()
	}

	qq := NewQuorumQueue(name, raft)
	m.queues[name] = qq
	return qq, nil
}

// Get looks up a quorum queue by name.
func (m *QuorumQueueManager) Get(name string) (*QuorumQueue, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.queues[name]
	return q, ok
}

// Delete removes a quorum queue.
func (m *QuorumQueueManager) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.queues[name]; !ok {
		return fmt.Errorf("quorum queue %q not found", name)
	}
	delete(m.queues, name)
	return nil
}

// List returns all quorum queues.
func (m *QuorumQueueManager) List() []*QuorumQueue {
	m.mu.RLock()
	out := make([]*QuorumQueue, 0, len(m.queues))
	for _, q := range m.queues {
		out = append(out, q)
	}
	m.mu.RUnlock()
	return out
}

// PromoteLeader forces a queue's Raft node to become leader.
// Used during failover testing.
func (m *QuorumQueueManager) PromoteLeader(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.queues[name]
	if !ok {
		return fmt.Errorf("quorum queue %q not found", name)
	}
	q.raft.BecomeLeader()
	return nil
}
