package server

import "testing"

// TestPullGet fetches a message from a non-empty queue.
func TestPullGet(t *testing.T) {
	store := NewMessageStore()
	tracker := NewDeliveryTracker(store)
	pc := NewPullConsumer(store, tracker)

	store.Enqueue("q1", NewMessage([]byte("msg1"), Properties{}))
	store.Enqueue("q1", NewMessage([]byte("msg2"), Properties{}))

	m, ok := pc.Get("q1", true, 1)
	if !ok {
		t.Fatal("expected message")
	}
	if string(m.Payload()) != "msg1" {
		t.Fatalf("body = %q, want msg1", m.Payload())
	}
	if store.Len("q1") != 1 {
		t.Fatalf("len = %d, want 1", store.Len("q1"))
	}
}

// TestPullGetEmpty returns false when queue is empty.
func TestPullGetEmpty(t *testing.T) {
	store := NewMessageStore()
	tracker := NewDeliveryTracker(store)
	pc := NewPullConsumer(store, tracker)

	_, ok := pc.Get("q1", true, 1)
	if ok {
		t.Fatal("expected no message")
	}
}

// TestPullGetAutoAck does not track delivery.
func TestPullGetAutoAck(t *testing.T) {
	store := NewMessageStore()
	tracker := NewDeliveryTracker(store)
	pc := NewPullConsumer(store, tracker)

	store.Enqueue("q1", NewMessage([]byte("ack"), Properties{}))
	pc.Get("q1", true, 1)
	if tracker.Count() != 0 {
		t.Fatalf("tracker = %d, want 0", tracker.Count())
	}
}

// TestPullGetNoAck tracks delivery for manual ack.
func TestPullGetNoAck(t *testing.T) {
	store := NewMessageStore()
	tracker := NewDeliveryTracker(store)
	pc := NewPullConsumer(store, tracker)

	store.Enqueue("q1", NewMessage([]byte("nack"), Properties{}))
	m, _ := pc.Get("q1", false, 1)
	if m.DeliveryTag() == 0 {
		t.Fatal("expected non-zero delivery tag")
	}
	if tracker.Count() != 1 {
		t.Fatalf("tracker = %d, want 1", tracker.Count())
	}
}
