// federation_manager.go manages federation links.
package server

import (
	"log"
	"sync"
)

// FederationManager holds registered federation configurations.
type FederationManager struct {
	configs map[string]*FederationConfig
	mu      sync.RWMutex
}

// NewFederationManager creates an empty federation manager.
func NewFederationManager() *FederationManager {
	return &FederationManager{
		configs: make(map[string]*FederationConfig),
	}
}

// Add registers a federation configuration.
func (m *FederationManager) Add(cfg *FederationConfig) {
	if cfg == nil || cfg.Name == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.configs[cfg.Name] = cfg
}

// Remove deletes a federation configuration by name.
func (m *FederationManager) Remove(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.configs[name]; !ok {
		return false
	}
	delete(m.configs, name)
	return true
}

// List returns a snapshot of all federation configurations.
func (m *FederationManager) List() []*FederationConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*FederationConfig, 0, len(m.configs))
	for _, cfg := range m.configs {
		out = append(out, cfg)
	}
	return out
}

// Count returns the number of registered federation configs.
func (m *FederationManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.configs)
}

// Get looks up a federation config by name.
func (m *FederationManager) Get(name string) (*FederationConfig, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cfg, ok := m.configs[name]
	return cfg, ok
}

// StartAll iterates over every federation configuration and starts
// the link. This is a placeholder stub; real implementation would
// establish upstream connections.
func (m *FederationManager) StartAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for name, cfg := range m.configs {
		log.Printf("federation start: name=%q upstreams=%v "+
			"exchange=%q queue=%q", name, cfg.Upstreams,
			cfg.Exchange, cfg.Queue)
	}
}
