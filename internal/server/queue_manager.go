// queue_manager.go manages queues within a virtual host.
package server

import (
	"fmt"
	"sync"
)

// QueueManager tracks all queues for a single vhost.
type QueueManager struct {
	queues map[string]*Queue
	mu     sync.RWMutex
}

// NewQueueManager creates an empty queue manager.
func NewQueueManager() *QueueManager {
	return &QueueManager{
		queues: make(map[string]*Queue),
	}
}

// Declare creates a queue or verifies an existing one matches.
func (m *QueueManager) Declare(
	name string,
	durable, exclusive, autoDelete bool,
	args map[string]interface{},
	owner *Connection,
) (*Queue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if q, ok := m.queues[name]; ok {
		if !q.MatchArgs(durable, exclusive, autoDelete, args) {
			return nil, fmt.Errorf(
				"queue %q already exists with different args",
				name,
			)
		}
		return q, nil // idempotent
	}

	q := NewQueue(name, durable, exclusive, autoDelete, args, owner)
	m.queues[name] = q
	return q, nil
}

// Delete removes a queue by name.
func (m *QueueManager) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.queues[name]; !ok {
		return fmt.Errorf("queue %q not found", name)
	}
	delete(m.queues, name)
	return nil
}

// Get looks up a queue by name.
func (m *QueueManager) Get(name string) (*Queue, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	q, ok := m.queues[name]
	return q, ok
}

// Count returns the number of queues.
func (m *QueueManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.queues)
}

// RemoveExclusive deletes all queues owned by the given connection.
func (m *QueueManager) RemoveExclusive(conn *Connection) {
	m.mu.Lock()
	var toDelete []string
	for name, q := range m.queues {
		if q.Owner() == conn {
			toDelete = append(toDelete, name)
		}
	}
	for _, name := range toDelete {
		delete(m.queues, name)
	}
	m.mu.Unlock()
}
