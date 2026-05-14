package server

import (
	"testing"
)

// TestMessageEnqueueDequeue verifies basic FIFO behavior.
func TestMessageEnqueueDequeue(t *testing.T) {
	store := NewMessageStore()
	m1 := NewMessage([]byte("first"), Properties{ContentType: "text/plain"})
	m2 := NewMessage([]byte("second"), Properties{ContentType: "text/plain"})

	store.Enqueue("q1", m1)
	store.Enqueue("q1", m2)
	if store.Len("q1") != 2 {
		t.Fatalf("len = %d, want 2", store.Len("q1"))
	}

	out, ok := store.Dequeue("q1")
	if !ok || string(out.Payload()) != "first" {
		t.Fatalf("expected first, got %v", out)
	}
	out, ok = store.Dequeue("q1")
	if !ok || string(out.Payload()) != "second" {
		t.Fatalf("expected second, got %v", out)
	}
	if store.Len("q1") != 0 {
		t.Fatalf("len = %d, want 0", store.Len("q1"))
	}
}

// TestMessagePeek returns the first message without removing it.
func TestMessagePeek(t *testing.T) {
	store := NewMessageStore()
	m := NewMessage([]byte("peek"), Properties{})
	store.Enqueue("q1", m)

	out, ok := store.Peek("q1")
	if !ok || string(out.Payload()) != "peek" {
		t.Fatal("peek mismatch")
	}
	if store.Len("q1") != 1 {
		t.Fatal("peek should not remove")
	}
}

// TestMessagePurge clears all messages.
func TestMessagePurge(t *testing.T) {
	store := NewMessageStore()
	store.Enqueue("q1", NewMessage([]byte("a"), Properties{}))
	store.Enqueue("q1", NewMessage([]byte("b"), Properties{}))
	store.Purge("q1")
	if store.Len("q1") != 0 {
		t.Fatal("purge failed")
	}
}

// TestMessageProperties verifies metadata round-trip.
func TestMessageProperties(t *testing.T) {
	props := Properties{
		ContentType:  "application/json",
		DeliveryMode: 2,
		Priority:     5,
	}
	m := NewMessage([]byte("body"), props)
	if m.Properties().ContentType != "application/json" {
		t.Fatal("content-type mismatch")
	}
	if m.Properties().DeliveryMode != 2 {
		t.Fatal("delivery-mode mismatch")
	}
}

// TestMessageDeliveryTag verifies tag assignment.
func TestMessageDeliveryTag(t *testing.T) {
	m := NewMessage([]byte("x"), Properties{})
	m.SetDeliveryTag(42)
	if m.DeliveryTag() != 42 {
		t.Fatalf("tag = %d, want 42", m.DeliveryTag())
	}
}

// TestMessageRoutingMeta sets exchange and routing key.
func TestMessageRoutingMeta(t *testing.T) {
	m := NewMessage([]byte("y"), Properties{})
	m.SetRoutingMeta("amq.direct", "news")
	if m.Exchange() != "amq.direct" {
		t.Fatal("exchange mismatch")
	}
	if m.RoutingKey() != "news" {
		t.Fatal("routing key mismatch")
	}
}
