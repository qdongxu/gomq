// memory_test.go tests the in-memory Store implementation.
package store

import (
	"context"
	"testing"
)

func TestMemoryStoreQueue(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	meta := QueueMeta{
		Name:       "q1",
		Durable:    true,
		Exclusive:  false,
		AutoDelete: false,
		Args:       map[string]interface{}{"x-max-length": 1000},
	}

	if err := s.SaveQueue(ctx, meta); err != nil {
		t.Fatalf("save queue: %v", err)
	}

	queues, err := s.LoadQueues(ctx)
	if err != nil {
		t.Fatalf("load queues: %v", err)
	}
	if len(queues) != 1 {
		t.Fatalf("len = %d, want 1", len(queues))
	}
	if queues[0].Name != "q1" {
		t.Fatalf("name = %q, want q1", queues[0].Name)
	}

	if err := s.DeleteQueue(ctx, "q1"); err != nil {
		t.Fatalf("delete queue: %v", err)
	}
	queues, _ = s.LoadQueues(ctx)
	if len(queues) != 0 {
		t.Fatalf("after delete len = %d, want 0", len(queues))
	}
}

func TestMemoryStoreExchange(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	meta := ExchangeMeta{
		Name:       "amq.direct",
		Type:       "direct",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
	}

	if err := s.SaveExchange(ctx, meta); err != nil {
		t.Fatalf("save exchange: %v", err)
	}

	exchanges, err := s.LoadExchanges(ctx)
	if err != nil {
		t.Fatalf("load exchanges: %v", err)
	}
	if len(exchanges) != 1 {
		t.Fatalf("len = %d, want 1", len(exchanges))
	}

	if err := s.DeleteExchange(ctx, "amq.direct"); err != nil {
		t.Fatalf("delete exchange: %v", err)
	}
	exchanges, _ = s.LoadExchanges(ctx)
	if len(exchanges) != 0 {
		t.Fatalf("after delete len = %d, want 0", len(exchanges))
	}
}

func TestMemoryStoreBinding(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	meta := BindingMeta{
		Exchange:   "amq.direct",
		Queue:      "q1",
		RoutingKey: "news",
		Args:       nil,
	}

	if err := s.SaveBinding(ctx, meta); err != nil {
		t.Fatalf("save binding: %v", err)
	}

	bindings, err := s.LoadBindings(ctx)
	if err != nil {
		t.Fatalf("load bindings: %v", err)
	}
	if len(bindings) != 1 {
		t.Fatalf("len = %d, want 1", len(bindings))
	}

	if err := s.DeleteBinding(ctx, "amq.direct", "q1", "news"); err != nil {
		t.Fatalf("delete binding: %v", err)
	}
	bindings, _ = s.LoadBindings(ctx)
	if len(bindings) != 0 {
		t.Fatalf("after delete len = %d, want 0", len(bindings))
	}
}

func TestMemoryStoreClose(t *testing.T) {
	s := NewMemoryStore()
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
