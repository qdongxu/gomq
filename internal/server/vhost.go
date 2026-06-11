// vhost.go manages virtual hosts for the broker.
package server

import (
	"sync"
	"time"
)

// VHost represents a virtual host namespace.
type VHost struct {
	Name        string
	Description string
	CreatedAt   time.Time
}

// VHostManager tracks all virtual hosts.
type VHostManager struct {
	vhosts map[string]*VHost
	mu     sync.RWMutex
}

// NewVHostManager creates a manager with only the default VHost "/".
func NewVHostManager() *VHostManager {
	m := &VHostManager{
		vhosts: make(map[string]*VHost),
	}
	m.vhosts["/"] = &VHost{
		Name:        "/",
		Description: "Default virtual host",
		CreatedAt:   time.Now(),
	}
	return m
}

// Create adds a new VHost. Returns false if the name already exists
// or if the name is "/" (default VHost is protected).
func (m *VHostManager) Create(name, description string) (*VHost, bool) {
	if name == "" || name == "/" {
		return nil, false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.vhosts[name]; ok {
		return nil, false
	}

	vh := &VHost{
		Name:        name,
		Description: description,
		CreatedAt:   time.Now(),
	}
	m.vhosts[name] = vh
	return vh, true
}

// Delete removes a VHost by name. Returns false if the VHost does
// not exist or if the name is "/" (default VHost cannot be deleted).
func (m *VHostManager) Delete(name string) bool {
	if name == "/" {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.vhosts[name]; !ok {
		return false
	}
	delete(m.vhosts, name)
	return true
}

// Get looks up a VHost by name.
func (m *VHostManager) Get(name string) (*VHost, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	vh, ok := m.vhosts[name]
	return vh, ok
}

// List returns all VHosts.
func (m *VHostManager) List() []*VHost {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*VHost, 0, len(m.vhosts))
	for _, vh := range m.vhosts {
		out = append(out, vh)
	}
	return out
}

// Count returns the number of VHosts.
func (m *VHostManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.vhosts)
}
