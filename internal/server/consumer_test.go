package server

import (
	"testing"
)

// TestSubscribeUnsubscribe verifies basic consumer lifecycle.
func TestSubscribeUnsubscribe(t *testing.T) {
	cm := NewConsumerManager()
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	ch, _ := NewChannelManager(10).Create(1, conn)

	c, err := cm.Subscribe("c1", "q1", ch, false, false, nil)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if c.Tag() != "c1" {
		t.Fatalf("tag = %q, want c1", c.Tag())
	}
	if cm.Count() != 1 {
		t.Fatalf("count = %d, want 1", cm.Count())
	}

	if err := cm.Unsubscribe("c1"); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	if cm.Count() != 0 {
		t.Fatalf("count = %d, want 0", cm.Count())
	}
}

// TestExclusiveConsumer prevents multiple exclusive consumers.
func TestExclusiveConsumer(t *testing.T) {
	cm := NewConsumerManager()
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	ch1, _ := NewChannelManager(10).Create(1, conn)
	ch2, _ := NewChannelManager(10).Create(2, conn)

	_, err := cm.Subscribe("c1", "q1", ch1, false, true, nil)
	if err != nil {
		t.Fatalf("first exclusive: %v", err)
	}
	_, err = cm.Subscribe("c2", "q1", ch2, false, true, nil)
	if err == nil {
		t.Fatal("expected error for second exclusive consumer")
	}
}

// TestMultipleConsumers allows non-exclusive consumers on same queue.
func TestMultipleConsumers(t *testing.T) {
	cm := NewConsumerManager()
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	ch1, _ := NewChannelManager(10).Create(1, conn)
	ch2, _ := NewChannelManager(10).Create(2, conn)

	_, err := cm.Subscribe("c1", "q1", ch1, false, false, nil)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	_, err = cm.Subscribe("c2", "q1", ch2, false, false, nil)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if cm.Count() != 2 {
		t.Fatalf("count = %d, want 2", cm.Count())
	}
}

// TestDuplicateTag rejects duplicate consumer tags.
func TestDuplicateTag(t *testing.T) {
	cm := NewConsumerManager()
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	ch, _ := NewChannelManager(10).Create(1, conn)

	_, _ = cm.Subscribe("c1", "q1", ch, false, false, nil)
	_, err := cm.Subscribe("c1", "q2", ch, false, false, nil)
	if err == nil {
		t.Fatal("expected error for duplicate tag")
	}
}

// TestGetConsumers returns consumers for a specific queue.
func TestGetConsumers(t *testing.T) {
	cm := NewConsumerManager()
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	ch, _ := NewChannelManager(10).Create(1, conn)

	_, _ = cm.Subscribe("c1", "q1", ch, false, false, nil)
	consumers := cm.GetConsumers("q1")
	if len(consumers) != 1 || consumers[0].Tag() != "c1" {
		t.Fatalf("consumers = %v, want [c1]", consumers)
	}
}
