// e2e_binding_manager.go manages exchange-to-exchange bindings.
package server

import (
	"fmt"
	"sync"
)

// E2EBindingManager tracks all E2E bindings and indexes them by
// source exchange for fast routing.
type E2EBindingManager struct {
	bySource map[string][]*E2EBinding
	mu       sync.RWMutex
}

// NewE2EBindingManager creates an empty E2E binding manager.
func NewE2EBindingManager() *E2EBindingManager {
	return &E2EBindingManager{
		bySource: make(map[string][]*E2EBinding),
	}
}

// Bind creates an E2E binding from source to destination.
func (m *E2EBindingManager) Bind(
	source, destination, routingKey string,
	args map[string]interface{},
) (*E2EBinding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for duplicate.
	for _, b := range m.bySource[source] {
		if b.Destination == destination &&
			b.RoutingKey == routingKey {
			return b, nil // idempotent
		}
	}

	b := NewE2EBinding(source, destination, routingKey, args)
	m.bySource[source] = append(m.bySource[source], b)
	return b, nil
}

// Unbind removes an E2E binding by source, destination, routing key.
func (m *E2EBindingManager) Unbind(
	source, destination, routingKey string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	list := m.bySource[source]
	for i, b := range list {
		if b.Destination == destination &&
			b.RoutingKey == routingKey {
			m.bySource[source] = append(
				list[:i], list[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("e2e binding not found")
}

// GetBindings returns all E2E bindings for a source exchange.
func (m *E2EBindingManager) GetBindings(
	source string,
) []*E2EBinding {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*E2EBinding, len(m.bySource[source]))
	copy(out, m.bySource[source])
	return out
}

// UnbindAllForExchange removes every E2E binding referencing the
// exchange as source or destination.
func (m *E2EBindingManager) UnbindAllForExchange(exchange string) {
	m.mu.Lock()
	delete(m.bySource, exchange)
	for src, list := range m.bySource {
		var keep []*E2EBinding
		for _, b := range list {
			if b.Destination != exchange {
				keep = append(keep, b)
			}
		}
		m.bySource[src] = keep
	}
	m.mu.Unlock()
}

// HasCycle reports whether adding an E2E binding would create a
// cycle in the exchange graph.
func (m *E2EBindingManager) HasCycle(
	source, destination string,
) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	visited := make(map[string]bool)
	var dfs func(string) bool
	dfs = func(node string) bool {
		if node == source {
			return true
		}
		if visited[node] {
			return false
		}
		visited[node] = true
		for _, b := range m.bySource[node] {
			if dfs(b.Destination) {
				return true
			}
		}
		return false
	}
	return dfs(destination)
}
