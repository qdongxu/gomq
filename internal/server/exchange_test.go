package server

import (
	"testing"
)

// TestExchangeDeclare creates a new exchange.
func TestExchangeDeclare(t *testing.T) {
	mgr := NewExchangeManager()
	ex, err := mgr.Declare("test.direct", ExchangeDirect, true, false, false, nil)
	if err != nil {
		t.Fatalf("declare: %v", err)
	}
	if ex.Name != "test.direct" {
		t.Fatalf("name = %q, want test.direct", ex.Name)
	}
	if ex.Type != ExchangeDirect {
		t.Fatalf("type = %q, want direct", ex.Type)
	}
}

// TestExchangeDeclareIdempotent allows re-declare with same type.
func TestExchangeDeclareIdempotent(t *testing.T) {
	mgr := NewExchangeManager()
	_, err := mgr.Declare("test", ExchangeDirect, true, false, false, nil)
	if err != nil {
		t.Fatalf("first declare: %v", err)
	}
	_, err = mgr.Declare("test", ExchangeDirect, true, false, false, nil)
	if err != nil {
		t.Fatalf("second declare: %v", err)
	}
}

// TestExchangeDeclareTypeConflict rejects different type re-declare.
func TestExchangeDeclareTypeConflict(t *testing.T) {
	mgr := NewExchangeManager()
	_, err := mgr.Declare("test", ExchangeDirect, true, false, false, nil)
	if err != nil {
		t.Fatalf("first declare: %v", err)
	}
	_, err = mgr.Declare("test", ExchangeFanout, true, false, false, nil)
	if err == nil {
		t.Fatal("expected error for type conflict")
	}
}

// TestExchangeDelete removes an exchange.
func TestExchangeDelete(t *testing.T) {
	mgr := NewExchangeManager()
	_, err := mgr.Declare("test", ExchangeDirect, true, false, false, nil)
	if err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := mgr.Delete("test"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := mgr.Get("test"); ok {
		t.Fatal("exchange should be deleted")
	}
}

// TestExchangeDeleteNotFound returns error for missing exchange.
func TestExchangeDeleteNotFound(t *testing.T) {
	mgr := NewExchangeManager()
	if err := mgr.Delete("missing"); err == nil {
		t.Fatal("expected error for missing exchange")
	}
}

// TestDirectRoute matches exact routing key.
func TestDirectRoute(t *testing.T) {
	ex := NewExchange("", ExchangeDirect, true, false, false, nil)
	bindings := []*Binding{
		{ExchangeName: "", QueueName: "q1", RoutingKey: "news"},
		{ExchangeName: "", QueueName: "q2", RoutingKey: "alert"},
		{ExchangeName: "", QueueName: "q3", RoutingKey: "news"},
	}
	router := ex.Router()
	queues := router.Route("news", bindings)
	if len(queues) != 2 {
		t.Fatalf("len = %d, want 2", len(queues))
	}
}

// TestFanoutRoute broadcasts to all queues.
func TestFanoutRoute(t *testing.T) {
	ex := NewExchange("", ExchangeFanout, true, false, false, nil)
	bindings := []*Binding{
		{ExchangeName: "", QueueName: "q1", RoutingKey: "ignore"},
		{ExchangeName: "", QueueName: "q2", RoutingKey: "ignore"},
	}
	router := ex.Router()
	queues := router.Route("anything", bindings)
	if len(queues) != 2 {
		t.Fatalf("len = %d, want 2", len(queues))
	}
}

// TestDefaultExchangeExists verifies built-in default exchanges.
func TestDefaultExchangeExists(t *testing.T) {
	mgr := NewExchangeManager()
	for _, name := range []string{
		"", "amq.direct", "amq.fanout", "amq.topic",
		"amq.headers", "amq.match",
	} {
		if _, ok := mgr.Get(name); !ok {
			t.Fatalf("default exchange %q missing", name)
		}
	}
}

// TestDefaultDirectRoute verifies "" direct exchange routes by queue name.
func TestDefaultDirectRoute(t *testing.T) {
	mgr := NewExchangeManager()
	ex, _ := mgr.Get("")
	bindings := []*Binding{
		{ExchangeName: "", QueueName: "my-queue", RoutingKey: "my-queue"},
	}
	router := ex.Router()
	queues := router.Route("my-queue", bindings)
	if len(queues) != 1 || queues[0] != "my-queue" {
		t.Fatalf("queues = %v, want [my-queue]", queues)
	}
}
