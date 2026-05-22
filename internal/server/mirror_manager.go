// mirror_manager.go manages the registry of mirrored queues.
package server

import (
	"log"
	"sync"
	"time"
)

// MirrorManager tracks all mirrored queues and orchestrates
// background synchronisation.
type MirrorManager struct {
	queues map[string]*MirroredQueue
	mu     sync.RWMutex
}

// NewMirrorManager creates an empty mirror manager.
func NewMirrorManager() *MirrorManager {
	return &MirrorManager{
		queues: make(map[string]*MirroredQueue),
	}
}

// Register creates a mirrored queue entry. If the queue name already
// exists the peer list is updated.
func (m *MirrorManager) Register(
	name string, q *Queue, nodes []string,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queues[name] = NewMirroredQueue(q, nodes)
}

// Unregister removes a mirrored queue by name.
func (m *MirrorManager) Unregister(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.queues, name)
}

// Get looks up a mirrored queue by name.
func (m *MirrorManager) Get(name string) (*MirroredQueue, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mq, ok := m.queues[name]
	return mq, ok
}

// Count returns the number of mirrored queues.
func (m *MirrorManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.queues)
}

// Names returns a snapshot of all mirrored queue names.
func (m *MirrorManager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.queues))
	for name := range m.queues {
		out = append(out, name)
	}
	return out
}

// SyncAll iterates over every mirrored queue and triggers a
// broadcast. In a real implementation this would sync pending
// messages to peers.
func (m *MirrorManager) SyncAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for name, mq := range m.queues {
		// Placeholder: real sync would push pending messages.
		_ = mq.Broadcast(nil)
		log.Printf("mirror sync: queue=%q peers=%v",
			name, mq.MirrorTo())
	}
}

// StartSyncLoop runs SyncAll in a background goroutine every
// interval. It returns a stop function.
func (m *MirrorManager) StartSyncLoop(
	interval time.Duration,
) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.SyncAll()
			case <-done:
				return
			}
		}
	}()
	return func() { close(done) }
}
