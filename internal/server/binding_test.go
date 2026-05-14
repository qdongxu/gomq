package server

import (
	"testing"
)

func setupRouter() (
	*MessageRouter,
	*ExchangeManager,
	*QueueManager,
	*BindingManager,
) {
	ex := NewExchangeManager()
	q := NewQueueManager()
	b := NewBindingManager()
	r := NewMessageRouter(ex, q, b)
	return r, ex, q, b
}

// TestBindUnbind creates and removes a binding.
func TestBindUnbind(t *testing.T) {
	_, _, qm, bm := setupRouter()
	_, _ = qm.Declare("q1", true, false, false, nil, nil)

	_, err := bm.Bind("amq.direct", "q1", "key1", nil)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	if len(bm.GetBindings("amq.direct")) != 1 {
		t.Fatal("expected 1 binding")
	}

	err = bm.Unbind("amq.direct", "q1", "key1")
	if err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if len(bm.GetBindings("amq.direct")) != 0 {
		t.Fatal("expected 0 bindings")
	}
}

// TestBindDuplicate allows idempotent binding.
func TestBindDuplicate(t *testing.T) {
	_, _, qm, bm := setupRouter()
	_, _ = qm.Declare("q1", true, false, false, nil, nil)

	_, _ = bm.Bind("amq.direct", "q1", "key1", nil)
	_, _ = bm.Bind("amq.direct", "q1", "key1", nil)

	if len(bm.GetBindings("amq.direct")) != 2 {
		t.Fatalf("expected 2 bindings, got %d", len(bm.GetBindings("amq.direct")))
	}
}

// TestCascadeDeleteExchange removes bindings when exchange deleted.
func TestCascadeDeleteExchange(t *testing.T) {
	r, _, qm, _ := setupRouter()
	_, _ = qm.Declare("q1", true, false, false, nil, nil)
	_, _ = r.bindings.Bind("amq.direct", "q1", "key1", nil)

	if err := r.DeleteExchange("amq.direct"); err != nil {
		t.Fatalf("delete exchange: %v", err)
	}
	if len(r.bindings.GetBindings("amq.direct")) != 0 {
		t.Fatal("bindings should be cleared")
	}
}

// TestCascadeDeleteQueue removes bindings when queue deleted.
func TestCascadeDeleteQueue(t *testing.T) {
	r, _, qm, _ := setupRouter()
	_, _ = qm.Declare("q1", true, false, false, nil, nil)
	_, _ = r.bindings.Bind("amq.direct", "q1", "key1", nil)

	if err := r.DeleteQueue("q1"); err != nil {
		t.Fatalf("delete queue: %v", err)
	}
	if len(r.bindings.GetBindingsForQueue("q1")) != 0 {
		t.Fatal("queue bindings should be cleared")
	}
}

// TestRouteDirect performs end-to-end direct routing.
func TestRouteDirect(t *testing.T) {
	r, _, qm, _ := setupRouter()
	_, _ = qm.Declare("q1", true, false, false, nil, nil)
	_, _ = qm.Declare("q2", true, false, false, nil, nil)
	_, _ = r.bindings.Bind("amq.direct", "q1", "news", nil)
	_, _ = r.bindings.Bind("amq.direct", "q2", "alert", nil)

	queues, err := r.Route("amq.direct", "news", nil)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if len(queues) != 1 || queues[0] != "q1" {
		t.Fatalf("queues = %v, want [q1]", queues)
	}
}

// TestRouteFanout performs end-to-end fanout routing.
func TestRouteFanout(t *testing.T) {
	r, _, qm, _ := setupRouter()
	_, _ = qm.Declare("q1", true, false, false, nil, nil)
	_, _ = qm.Declare("q2", true, false, false, nil, nil)
	_, _ = r.bindings.Bind("amq.fanout", "q1", "", nil)
	_, _ = r.bindings.Bind("amq.fanout", "q2", "", nil)

	queues, err := r.Route("amq.fanout", "ignored", nil)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if len(queues) != 2 {
		t.Fatalf("len = %d, want 2", len(queues))
	}
}
