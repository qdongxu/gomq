// binding_manager.go manages exchange-to-queue bindings.
package server

import (
	"fmt"
	"sync"
)

// BindingManager tracks all bindings and indexes them by exchange
// and by queue for fast lookups and cascade deletion.
type BindingManager struct {
	byExchange map[string][]*Binding
	byQueue    map[string][]*Binding
	mu         sync.RWMutex
}

// NewBindingManager creates an empty binding manager.
func NewBindingManager() *BindingManager {
	return &BindingManager{
		byExchange: make(map[string][]*Binding),
		byQueue:    make(map[string][]*Binding),
	}
}

// Bind creates a binding between an exchange and a queue.
func (m *BindingManager) Bind(
	exchange, queue, routingKey string,
	args map[string]interface{},
) (*Binding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b := NewBinding(exchange, queue, routingKey, args)
	m.byExchange[exchange] = append(m.byExchange[exchange], b)
	m.byQueue[queue] = append(m.byQueue[queue], b)
	return b, nil
}

// Unbind removes a binding by exchange, queue, and routing key.
func (m *BindingManager) Unbind(
	exchange, queue, routingKey string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	found := m.removeFrom(
		m.byExchange, exchange,
		queue, routingKey,
	)
	m.removeFrom(
		m.byQueue, queue,
		exchange, routingKey,
	)
	if !found {
		return fmt.Errorf("binding not found")
	}
	return nil
}

// removeFrom deletes a binding from one of the index maps.
func (m *BindingManager) removeFrom(
	idx map[string][]*Binding,
	key, targetQueue, targetKey string,
) bool {
	list := idx[key]
	for i, b := range list {
		if b.QueueName == targetQueue && b.RoutingKey == targetKey {
			idx[key] = append(list[:i], list[i+1:]...)
			return true
		}
	}
	return false
}

// GetBindings returns all bindings for an exchange.
func (m *BindingManager) GetBindings(
	exchangeName string,
) []*Binding {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Binding, len(m.byExchange[exchangeName]))
	copy(out, m.byExchange[exchangeName])
	return out
}

// GetBindingsForQueue returns all bindings for a queue.
func (m *BindingManager) GetBindingsForQueue(
	queueName string,
) []*Binding {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Binding, len(m.byQueue[queueName]))
	copy(out, m.byQueue[queueName])
	return out
}

// UnbindAllForExchange removes every binding referencing the exchange.
func (m *BindingManager) UnbindAllForExchange(exchange string) {
	m.mu.Lock()
	list := m.byExchange[exchange]
	delete(m.byExchange, exchange)
	m.mu.Unlock()

	m.mu.Lock()
	for _, b := range list {
		m.removeFrom(
			m.byQueue, b.QueueName,
			b.ExchangeName, b.RoutingKey,
		)
	}
	m.mu.Unlock()
}

// UnbindAllForQueue removes every binding referencing the queue.
func (m *BindingManager) UnbindAllForQueue(queue string) {
	m.mu.Lock()
	list := m.byQueue[queue]
	delete(m.byQueue, queue)
	m.mu.Unlock()

	m.mu.Lock()
	for _, b := range list {
		m.removeFrom(
			m.byExchange, b.ExchangeName,
			b.QueueName, b.RoutingKey,
		)
	}
	m.mu.Unlock()
}
