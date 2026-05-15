// store.go defines the persistence interface for broker metadata.
package store

import (
	"context"
	"time"
)

// QueueMeta holds the serializable metadata for a queue.
type QueueMeta struct {
	Name       string                 `json:"name"`
	Durable    bool                   `json:"durable"`
	Exclusive  bool                   `json:"exclusive"`
	AutoDelete bool                   `json:"auto_delete"`
	Args       map[string]interface{} `json:"args,omitempty"`
}

// ExchangeMeta holds the serializable metadata for an exchange.
type ExchangeMeta struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Durable    bool                   `json:"durable"`
	AutoDelete bool                   `json:"auto_delete"`
	Internal   bool                   `json:"internal"`
	Args       map[string]interface{} `json:"args,omitempty"`
}

// BindingMeta holds the serializable metadata for a binding.
type BindingMeta struct {
	Exchange   string                 `json:"exchange"`
	Queue      string                 `json:"queue"`
	RoutingKey string                 `json:"routing_key"`
	Args       map[string]interface{} `json:"args,omitempty"`
}

// Store is the persistence abstraction for broker metadata.
type Store interface {
	// SaveQueue persists queue metadata.
	SaveQueue(ctx context.Context, meta QueueMeta) error
	// DeleteQueue removes a queue from persistent storage.
	DeleteQueue(ctx context.Context, name string) error
	// LoadQueues returns all persisted queues.
	LoadQueues(ctx context.Context) ([]QueueMeta, error)

	// SaveExchange persists exchange metadata.
	SaveExchange(ctx context.Context, meta ExchangeMeta) error
	// DeleteExchange removes an exchange from persistent storage.
	DeleteExchange(ctx context.Context, name string) error
	// LoadExchanges returns all persisted exchanges.
	LoadExchanges(ctx context.Context) ([]ExchangeMeta, error)

	// SaveBinding persists a binding.
	SaveBinding(ctx context.Context, meta BindingMeta) error
	// DeleteBinding removes a binding from persistent storage.
	DeleteBinding(ctx context.Context, exchange, queue, routingKey string) error
	// LoadBindings returns all persisted bindings.
	LoadBindings(ctx context.Context) ([]BindingMeta, error)

	// Close releases resources held by the store.
	Close() error
}

// DefaultTimeout is the context timeout for store operations.
const DefaultTimeout = 5 * time.Second
