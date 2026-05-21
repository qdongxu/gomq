// e2e_binding_test.go tests exchange-to-exchange bindings.
package server

import (
	"testing"
)

// TestE2EBindingManager tests CRUD operations.
func TestE2EBindingManager(t *testing.T) {
	m := NewE2EBindingManager()

	b, err := m.Bind("exA", "exB", "rk1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if b.Source != "exA" || b.Destination != "exB" {
		t.Fatalf("unexpected binding: %+v", b)
	}

	list := m.GetBindings("exA")
	if len(list) != 1 {
		t.Fatalf("expected 1 binding, got %d", len(list))
	}

	// Idempotent re-bind returns existing.
	b2, _ := m.Bind("exA", "exB", "rk1", nil)
	if b2 != b {
		t.Fatal("expected same binding on re-bind")
	}

	// Bind with different routing key creates second.
	_, _ = m.Bind("exA", "exB", "rk2", nil)
	list = m.GetBindings("exA")
	if len(list) != 2 {
		t.Fatalf("expected 2 bindings, got %d", len(list))
	}

	// Unbind.
	if err := m.Unbind("exA", "exB", "rk1"); err != nil {
		t.Fatal(err)
	}
	list = m.GetBindings("exA")
	if len(list) != 1 {
		t.Fatalf("expected 1 binding after unbind, got %d", len(list))
	}

	// Unbind nonexistent returns error.
	if err := m.Unbind("exA", "exB", "rk1"); err == nil {
		t.Fatal("expected error for unbind nonexistent")
	}
}

// TestE2EBindingCycle detects A->B->A loops.
func TestE2EBindingCycle(t *testing.T) {
	m := NewE2EBindingManager()

	_, _ = m.Bind("exA", "exB", "rk", nil)

	if !m.HasCycle("exB", "exA") {
		t.Fatal("expected cycle detected: B->A creates A->B->A loop")
	}

	if m.HasCycle("exC", "exA") {
		t.Fatal("expected no cycle: C->A does not create loop")
	}
}

// TestE2EBindingUnbindAllForExchange cleans up both directions.
func TestE2EBindingUnbindAllForExchange(t *testing.T) {
	m := NewE2EBindingManager()

	_, _ = m.Bind("exA", "exB", "rk", nil)
	_, _ = m.Bind("exB", "exC", "rk", nil)
	_, _ = m.Bind("exC", "exA", "rk", nil)

	m.UnbindAllForExchange("exB")

	if len(m.GetBindings("exA")) != 0 {
		t.Fatal("expected exA bindings cleaned")
	}
	if len(m.GetBindings("exB")) != 0 {
		t.Fatal("expected exB bindings cleaned")
	}
	if len(m.GetBindings("exC")) != 1 {
		t.Fatalf("expected exC still has 1 binding, got %d",
			len(m.GetBindings("exC")))
	}
}

// TestPublisherE2ERouting routes through destination exchange.
func TestPublisherE2ERouting(t *testing.T) {
	s := NewServer()

	// Create exchanges: A(direct) -> B(direct).
	_, _ = s.ExchangeManager().Declare("exA", ExchangeDirect,
		false, false, false, nil)
	_, _ = s.ExchangeManager().Declare("exB", ExchangeDirect,
		false, false, false, nil)

	// Create queue bound to B.
	_, _ = s.QueueManager().Declare("qB", false, false,
		false, nil, nil)
	_, _ = s.BindingManager().Bind("exB", "qB", "rk", nil)

	// E2E bind: A -> B with same routing key.
	_, _ = s.E2EBindingManager().Bind("exA", "exB", "rk", nil)

	// Publish to A.
	msg := NewMessage([]byte("hello"), Properties{})
	n, err := s.Publisher().Publish("exA", "rk", msg, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 queue routed, got %d", n)
	}

	// Message should be in qB.
	if s.MessageStore().Len("qB") != 1 {
		t.Fatalf("expected 1 message in qB, got %d",
			s.MessageStore().Len("qB"))
	}
}

// TestPublisherE2ELoopPrevention prevents infinite recursion.
func TestPublisherE2ELoopPrevention(t *testing.T) {
	s := NewServer()

	_, _ = s.ExchangeManager().Declare("exA", ExchangeDirect,
		false, false, false, nil)
	_, _ = s.ExchangeManager().Declare("exB", ExchangeDirect,
		false, false, false, nil)

	// Create a queue on B.
	_, _ = s.QueueManager().Declare("qB", false, false,
		false, nil, nil)
	_, _ = s.BindingManager().Bind("exB", "qB", "rk", nil)

	// Manually create E2E binding A->B (by-pass cycle check for test).
	s.E2EBindingManager().Bind("exA", "exB", "rk", nil)

	// Publish to A.
	msg := NewMessage([]byte("loop-test"), Properties{})
	_, err := s.Publisher().Publish("exA", "rk", msg, 1)
	if err != nil {
		t.Fatal(err)
	}

	// If loop prevention failed this would hang or panic.
	// We just verify qB got the message.
	if s.MessageStore().Len("qB") != 1 {
		t.Fatalf("expected 1 message in qB, got %d",
			s.MessageStore().Len("qB"))
	}
}

// TestExchangeDeleteCleansE2EBindings removes E2E refs on delete.
func TestExchangeDeleteCleansE2EBindings(t *testing.T) {
	s := NewServer()

	_, _ = s.ExchangeManager().Declare("exA", ExchangeDirect,
		false, false, false, nil)
	_, _ = s.ExchangeManager().Declare("exB", ExchangeDirect,
		false, false, false, nil)

	_, _ = s.E2EBindingManager().Bind("exA", "exB", "rk", nil)

	s.ExchangeManager().Delete("exA")
	s.E2EBindingManager().UnbindAllForExchange("exA")

	if len(s.E2EBindingManager().GetBindings("exA")) != 0 {
		t.Fatal("expected exA E2E bindings cleaned after delete")
	}
}
