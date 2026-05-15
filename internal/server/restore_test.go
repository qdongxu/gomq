// restore_test.go tests loading persisted state from a Store.
package server

import (
	"context"
	"testing"

	"github.com/qdongxu/gomq/internal/store"
)

func TestRestoreFromStore(t *testing.T) {
	meta := store.NewMemoryStore()
	ctx := context.Background()

	// Create first server, declare state, which persists to meta.
	s1 := NewServerWithStore(meta)
	q, err := s1.QueueManager().Declare("q1", true, false, false,
		nil, nil)
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if q == nil {
		t.Fatal("queue is nil")
	}

	ex, err := s1.ExchangeManager().Declare("ex1", ExchangeDirect,
		true, false, false, nil)
	if err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if ex == nil {
		t.Fatal("exchange is nil")
	}

	_, err = s1.BindingManager().Bind("ex1", "q1", "rk", nil)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}

	// Create second server with the same store and restore.
	s2 := NewServerWithStore(meta)
	if err := s2.RestoreFromStore(ctx); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify queue restored.
	if s2.QueueManager().Count() != 1 {
		t.Fatalf("queue count = %d, want 1", s2.QueueManager().Count())
	}
	q2, ok := s2.QueueManager().Get("q1")
	if !ok {
		t.Fatal("restored queue not found")
	}
	if !q2.Durable {
		t.Fatal("restored queue should be durable")
	}

	// Verify exchange restored.
	if s2.ExchangeManager().Count() != 7 { // 6 defaults + ex1
		t.Fatalf("exchange count = %d, want 7", s2.ExchangeManager().Count())
	}
	ex2, ok := s2.ExchangeManager().Get("ex1")
	if !ok {
		t.Fatal("restored exchange not found")
	}
	if ex2.Type != ExchangeDirect {
		t.Fatalf("type = %v, want direct", ex2.Type)
	}

	// Verify binding restored.
	bindings := s2.BindingManager().GetBindings("ex1")
	if len(bindings) != 1 {
		t.Fatalf("binding count = %d, want 1", len(bindings))
	}
	if bindings[0].QueueName != "q1" {
		t.Fatalf("queue = %q, want q1", bindings[0].QueueName)
	}
}

func TestRestoreFromStoreNil(t *testing.T) {
	s := NewServer()
	ctx := context.Background()
	if err := s.RestoreFromStore(ctx); err != nil {
		t.Fatalf("restore with nil store: %v", err)
	}
	if s.QueueManager().Count() != 0 {
		t.Fatalf("queue count = %d, want 0", s.QueueManager().Count())
	}
}
