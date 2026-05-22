// mirror_queue.go defines the simplified mirrored queue abstraction.
// A mirrored queue exists on multiple nodes; messages are broadcast
// to all peers in the mirror set.
package server

import (
	"sync"
)

// MirroredQueue wraps a Queue with a set of peer node IDs that
// maintain a replica of this queue.
type MirroredQueue struct {
	*Queue
	mu       sync.RWMutex
	mirrorTo []string // peer node IDs
}

// NewMirroredQueue creates a mirrored queue from an existing Queue
// and a list of peer node IDs.
func NewMirroredQueue(
	q *Queue, nodes []string,
) *MirroredQueue {
	return &MirroredQueue{
		Queue:    q,
		mirrorTo: append([]string(nil), nodes...),
	}
}

// MirrorTo returns a copy of the peer node IDs.
func (m *MirroredQueue) MirrorTo() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, len(m.mirrorTo))
	copy(out, m.mirrorTo)
	return out
}

// SetMirrorTo replaces the peer node IDs.
func (m *MirroredQueue) SetMirrorTo(nodes []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mirrorTo = append([]string(nil), nodes...)
}

// AddMirror adds a peer node ID if it is not already present.
func (m *MirroredQueue) AddMirror(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range m.mirrorTo {
		if id == nodeID {
			return
		}
	}
	m.mirrorTo = append(m.mirrorTo, nodeID)
}

// RemoveMirror removes a peer node ID.
func (m *MirroredQueue) RemoveMirror(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, id := range m.mirrorTo {
		if id == nodeID {
			m.mirrorTo = append(m.mirrorTo[:i], m.mirrorTo[i+1:]...)
			return
		}
	}
}

// Broadcast sends the message to all peer nodes in the mirror set.
// This is a simplified placeholder implementation; in production it
// would perform an RPC call to each peer.
func (m *MirroredQueue) Broadcast(msg *Message) error {
	peers := m.MirrorTo()
	if len(peers) == 0 {
		return nil
	}
	// Placeholder: real implementation would RPC to each peer.
	_ = msg
	_ = peers
	return nil
}
