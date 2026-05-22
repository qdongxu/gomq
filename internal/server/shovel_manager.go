// shovel_manager.go manages running shovel instances.
package server

import (
	"log"
	"sync"
)

// ShovelManager tracks all shovels and orchestrates their lifecycle.
type ShovelManager struct {
	shovels map[string]*Shovel
	mu      sync.RWMutex
}

// NewShovelManager creates an empty shovel manager.
func NewShovelManager() *ShovelManager {
	return &ShovelManager{
		shovels: make(map[string]*Shovel),
	}
}

// Add registers a shovel.
func (m *ShovelManager) Add(s *Shovel) {
	if s == nil || s.Name == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shovels[s.Name] = s
}

// Remove deletes a shovel by name.
func (m *ShovelManager) Remove(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.shovels[name]; !ok {
		return false
	}
	delete(m.shovels, name)
	return true
}

// List returns a snapshot of all shovels.
func (m *ShovelManager) List() []*Shovel {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Shovel, 0, len(m.shovels))
	for _, s := range m.shovels {
		out = append(out, s)
	}
	return out
}

// Count returns the number of registered shovels.
func (m *ShovelManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.shovels)
}

// Get looks up a shovel by name.
func (m *ShovelManager) Get(name string) (*Shovel, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.shovels[name]
	return s, ok
}

// StartAll runs every registered shovel. It is a placeholder stub;
// production code would start background goroutines for each link.
func (m *ShovelManager) StartAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for name, s := range m.shovels {
		if err := s.Run(); err != nil {
			log.Printf("shovel %s run: %v", name, err)
			continue
		}
		log.Printf("shovel started: %s %s -> %s (status=%s)",
			name, s.Source, s.Dest, s.Status().String())
	}
}
