package server

import (
	"testing"
)

func setupTracker() (*DeliveryTracker, *MessageStore) {
	store := NewMessageStore()
	return NewDeliveryTracker(store), store
}

// TestAck removes a delivery from tracking.
func TestAck(t *testing.T) {
	tracker, _ := setupTracker()
	msg := NewMessage([]byte("a"), Properties{})
	tracker.Record(1, msg, "q1", 1)
	if tracker.Count() != 1 {
		t.Fatalf("count = %d, want 1", tracker.Count())
	}
	if err := tracker.Ack(1, 1); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if tracker.Count() != 0 {
		t.Fatalf("count = %d, want 0", tracker.Count())
	}
}

// TestNackRequeue returns message to the queue.
func TestNackRequeue(t *testing.T) {
	tracker, store := setupTracker()
	msg := NewMessage([]byte("b"), Properties{})
	tracker.Record(1, msg, "q1", 1)
	if err := tracker.Nack(1, 1, true); err != nil {
		t.Fatalf("nack: %v", err)
	}
	if store.Len("q1") != 1 {
		t.Fatalf("queue len = %d, want 1", store.Len("q1"))
	}
}

// TestNackDiscard drops the message.
func TestNackDiscard(t *testing.T) {
	tracker, store := setupTracker()
	msg := NewMessage([]byte("c"), Properties{})
	tracker.Record(1, msg, "q1", 1)
	if err := tracker.Nack(1, 1, false); err != nil {
		t.Fatalf("nack: %v", err)
	}
	if store.Len("q1") != 0 {
		t.Fatalf("queue len = %d, want 0", store.Len("q1"))
	}
}

// TestRejectRequeue returns message to the queue.
func TestRejectRequeue(t *testing.T) {
	tracker, store := setupTracker()
	msg := NewMessage([]byte("d"), Properties{})
	tracker.Record(1, msg, "q1", 1)
	if err := tracker.Reject(1, 1, true); err != nil {
		t.Fatalf("reject: %v", err)
	}
	if store.Len("q1") != 1 {
		t.Fatalf("queue len = %d, want 1", store.Len("q1"))
	}
}

// TestMultipleDelivery tracks several messages independently.
func TestMultipleDelivery(t *testing.T) {
	tracker, _ := setupTracker()
	tracker.Record(1, NewMessage([]byte("x"), Properties{}), "q1", 1)
	tracker.Record(2, NewMessage([]byte("y"), Properties{}), "q1", 1)
	tracker.Record(3, NewMessage([]byte("z"), Properties{}), "q2", 1)

	if tracker.Count() != 3 {
		t.Fatalf("count = %d, want 3", tracker.Count())
	}
	unacked := tracker.GetUnacked(1)
	if len(unacked) != 3 {
		t.Fatalf("unacked = %d, want 3", len(unacked))
	}

	if err := tracker.Ack(2, 1); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if tracker.Count() != 2 {
		t.Fatalf("count = %d, want 2", tracker.Count())
	}
}

// TestNackAll requeues every message for a channel.
func TestNackAll(t *testing.T) {
	tracker, store := setupTracker()
	tracker.Record(1, NewMessage([]byte("a"), Properties{}), "q1", 1)
	tracker.Record(2, NewMessage([]byte("b"), Properties{}), "q1", 1)
	tracker.NackAll(1, true)
	if store.Len("q1") != 2 {
		t.Fatalf("queue len = %d, want 2", store.Len("q1"))
	}
	if tracker.Count() != 0 {
		t.Fatalf("count = %d, want 0", tracker.Count())
	}
}
