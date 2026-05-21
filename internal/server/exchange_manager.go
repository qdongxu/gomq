// exchange_manager.go manages exchanges within a virtual host.
package server

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/qdongxu/gomq/internal/store"
)

// ExchangeManager tracks all exchanges for a single vhost.
type ExchangeManager struct {
	exchanges map[string]*Exchange
	metaStore store.Store
	mu        sync.RWMutex
}

// NewExchangeManager creates a manager with default exchanges.
func NewExchangeManager() *ExchangeManager {
	return NewExchangeManagerWithStore(nil)
}

// NewExchangeManagerWithStore creates a manager with an optional
// backing store.
func NewExchangeManagerWithStore(metaStore store.Store) *ExchangeManager {
	m := &ExchangeManager{
		exchanges: make(map[string]*Exchange),
		metaStore: metaStore,
	}
	m.initDefaults()
	return m
}

// Declare creates an exchange or verifies an existing one matches.
func (m *ExchangeManager) Declare(
	name string,
	exType ExchangeType,
	durable, autoDelete, internal bool,
	args map[string]interface{},
) (*Exchange, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ex, ok := m.exchanges[name]; ok {
		if ex.Type != exType {
			return nil, fmt.Errorf(
				"exchange %q already exists with type %q",
				name, ex.Type,
			)
		}
		return ex, nil // idempotent
	}

	ex := NewExchange(name, exType, durable, autoDelete, internal, args)
	m.exchanges[name] = ex

	if m.metaStore != nil {
		ctx, cancel := context.WithTimeout(
			context.Background(), store.DefaultTimeout)
		err := m.metaStore.SaveExchange(ctx, store.ExchangeMeta{
			Name:       name,
			Type:       string(exType),
			Durable:    durable,
			AutoDelete: autoDelete,
			Internal:   internal,
			Args:       args,
		})
		cancel()
		if err != nil {
			log.Printf("save exchange %q: %v", name, err)
		}
	}
	return ex, nil
}

// Delete removes an exchange by name.
func (m *ExchangeManager) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.exchanges[name]; !ok {
		return fmt.Errorf("exchange %q not found", name)
	}
	delete(m.exchanges, name)

	if m.metaStore != nil {
		ctx, cancel := context.WithTimeout(
			context.Background(), store.DefaultTimeout)
		err := m.metaStore.DeleteExchange(ctx, name)
		cancel()
		if err != nil {
			log.Printf("delete exchange %q: %v", name, err)
		}
	}
	return nil
}

// Get looks up an exchange by name.
func (m *ExchangeManager) Get(name string) (*Exchange, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ex, ok := m.exchanges[name]
	return ex, ok
}

// Count returns the number of exchanges.
func (m *ExchangeManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.exchanges)
}

// List returns a snapshot of all exchanges.
func (m *ExchangeManager) List() []*Exchange {
	m.mu.RLock()
	out := make([]*Exchange, 0, len(m.exchanges))
	for _, ex := range m.exchanges {
		out = append(out, ex)
	}
	m.mu.RUnlock()
	return out
}

// initDefaults creates the built-in exchanges required by AMQP.
func (m *ExchangeManager) initDefaults() {
	m.exchanges[""] = NewExchange("", ExchangeDirect, true, false, false, nil)
	m.exchanges["amq.direct"] = NewExchange(
		"amq.direct", ExchangeDirect, true, false, false, nil,
	)
	m.exchanges["amq.fanout"] = NewExchange(
		"amq.fanout", ExchangeFanout, true, false, false, nil,
	)
	m.exchanges["amq.topic"] = NewExchange(
		"amq.topic", ExchangeTopic, true, false, false, nil,
	)
	m.exchanges["amq.headers"] = NewExchange(
		"amq.headers", ExchangeHeaders, true, false, false, nil,
	)
	m.exchanges["amq.match"] = NewExchange(
		"amq.match", ExchangeHeaders, true, false, false, nil,
	)
}
