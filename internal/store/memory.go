// memory.go provides an in-memory Store implementation for testing.
package store

import (
	"context"
	"fmt"
	"sync"
)

// MemoryStore is a non-persistent Store backed by maps.
type MemoryStore struct {
	queues    map[string]QueueMeta
	exchanges map[string]ExchangeMeta
	bindings  map[string]BindingMeta
	mu        sync.RWMutex
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		queues:    make(map[string]QueueMeta),
		exchanges: make(map[string]ExchangeMeta),
		bindings:  make(map[string]BindingMeta),
	}
}

// SaveQueue stores queue metadata in memory.
func (s *MemoryStore) SaveQueue(
	_ context.Context,
	meta QueueMeta,
) error {
	s.mu.Lock()
	s.queues[meta.Name] = meta
	s.mu.Unlock()
	return nil
}

// DeleteQueue removes a queue from memory.
func (s *MemoryStore) DeleteQueue(
	_ context.Context,
	name string,
) error {
	s.mu.Lock()
	delete(s.queues, name)
	s.mu.Unlock()
	return nil
}

// LoadQueues returns all queues from memory.
func (s *MemoryStore) LoadQueues(
	_ context.Context,
) ([]QueueMeta, error) {
	s.mu.RLock()
	out := make([]QueueMeta, 0, len(s.queues))
	for _, meta := range s.queues {
		out = append(out, meta)
	}
	s.mu.RUnlock()
	return out, nil
}

// SaveExchange stores exchange metadata in memory.
func (s *MemoryStore) SaveExchange(
	_ context.Context,
	meta ExchangeMeta,
) error {
	s.mu.Lock()
	s.exchanges[meta.Name] = meta
	s.mu.Unlock()
	return nil
}

// DeleteExchange removes an exchange from memory.
func (s *MemoryStore) DeleteExchange(
	_ context.Context,
	name string,
) error {
	s.mu.Lock()
	delete(s.exchanges, name)
	s.mu.Unlock()
	return nil
}

// LoadExchanges returns all exchanges from memory.
func (s *MemoryStore) LoadExchanges(
	_ context.Context,
) ([]ExchangeMeta, error) {
	s.mu.RLock()
	out := make([]ExchangeMeta, 0, len(s.exchanges))
	for _, meta := range s.exchanges {
		out = append(out, meta)
	}
	s.mu.RUnlock()
	return out, nil
}

// bindingKey builds a unique key for a binding.
func bindingKey(exchange, queue, routingKey string) string {
	return fmt.Sprintf("%s|%s|%s", exchange, queue, routingKey)
}

// SaveBinding stores binding metadata in memory.
func (s *MemoryStore) SaveBinding(
	_ context.Context,
	meta BindingMeta,
) error {
	s.mu.Lock()
	s.bindings[bindingKey(
		meta.Exchange, meta.Queue, meta.RoutingKey,
	)] = meta
	s.mu.Unlock()
	return nil
}

// DeleteBinding removes a binding from memory.
func (s *MemoryStore) DeleteBinding(
	_ context.Context,
	exchange, queue, routingKey string,
) error {
	s.mu.Lock()
	delete(s.bindings, bindingKey(exchange, queue, routingKey))
	s.mu.Unlock()
	return nil
}

// LoadBindings returns all bindings from memory.
func (s *MemoryStore) LoadBindings(
	_ context.Context,
) ([]BindingMeta, error) {
	s.mu.RLock()
	out := make([]BindingMeta, 0, len(s.bindings))
	for _, meta := range s.bindings {
		out = append(out, meta)
	}
	s.mu.RUnlock()
	return out, nil
}

// Close is a no-op for MemoryStore.
func (s *MemoryStore) Close() error {
	return nil
}
