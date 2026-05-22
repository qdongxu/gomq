// plugin_manager.go manages the lifecycle of registered plugins.
package server

import (
	"log"
	"sync"

	"github.com/qdongxu/gomq/pkg/plugin"
)

// PluginManager holds registered plugins and orchestrates their
// initialisation.
type PluginManager struct {
	plugins []plugin.Plugin
	mu      sync.RWMutex
}

// NewPluginManager creates an empty plugin manager.
func NewPluginManager() *PluginManager {
	return &PluginManager{
		plugins: make([]plugin.Plugin, 0),
	}
}

// Register adds a plugin to the manager. Registering a nil plugin is
// silently ignored.
func (m *PluginManager) Register(p plugin.Plugin) {
	if p == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.plugins = append(m.plugins, p)
}

// LoadAll returns a list of loaded plugin names. It does not call
// Init; use InitAll for that.
func (m *PluginManager) LoadAll() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, len(m.plugins))
	for i, p := range m.plugins {
		out[i] = p.Name()
	}
	return out
}

// InitAll calls Init on every registered plugin. Errors are logged
// but do not stop subsequent plugins from initialising.
func (m *PluginManager) InitAll(srv *Server) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.plugins {
		if err := p.Init(srv); err != nil {
			log.Printf("plugin %s init: %v", p.Name(), err)
		}
	}
}

// Count returns the number of registered plugins.
func (m *PluginManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.plugins)
}

// Names returns the names of all registered plugins.
func (m *PluginManager) Names() []string {
	return m.LoadAll()
}

// Get looks up a plugin by name.
func (m *PluginManager) Get(name string) (plugin.Plugin, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.plugins {
		if p.Name() == name {
			return p, true
		}
	}
	return nil, false
}

// Unregister removes a plugin by name. Returns true if a plugin was
// removed.
func (m *PluginManager) Unregister(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, p := range m.plugins {
		if p.Name() == name {
			m.plugins = append(m.plugins[:i], m.plugins[i+1:]...)
			return true
		}
	}
	return false
}
