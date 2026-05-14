package server

import (
	"testing"
)

func setupPublisher() *Publisher {
	ex := NewExchangeManager()
	qm := NewQueueManager()
	bm := NewBindingManager()
	store := NewMessageStore()
	cm := NewConsumerManager()
	tracker := NewDeliveryTracker(store)
	return NewPublisher(ex, qm, bm, store, cm, tracker)
}

// TestPublishDirect routes via direct exchange.
func TestPublishDirect(t *testing.T) {
	p := setupPublisher()
	_, _ = p.exchanges.Declare(
		"amq.direct", ExchangeDirect,
		false, false, false, nil,
	)
	_, _ = p.queues.Declare("q1", false, false, false, nil, nil)
	_, _ = p.bindings.Bind("amq.direct", "q1", "news", nil)

	msg := NewMessage([]byte("hello"), Properties{})
	msg.SetRoutingMeta("amq.direct", "news")
	if err := p.Publish("amq.direct", "news", msg, 1); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if p.store.Len("q1") != 1 {
		t.Fatalf("queue len = %d, want 1", p.store.Len("q1"))
	}
}

// TestPublishFanout broadcasts to all bound queues.
func TestPublishFanout(t *testing.T) {
	p := setupPublisher()
	_, _ = p.exchanges.Declare(
		"amq.fanout", ExchangeFanout,
		false, false, false, nil,
	)
	_, _ = p.queues.Declare("q1", false, false, false, nil, nil)
	_, _ = p.queues.Declare("q2", false, false, false, nil, nil)
	_, _ = p.bindings.Bind("amq.fanout", "q1", "", nil)
	_, _ = p.bindings.Bind("amq.fanout", "q2", "", nil)

	msg := NewMessage([]byte("broadcast"), Properties{})
	if err := p.Publish("amq.fanout", "", msg, 1); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if p.store.Len("q1") != 1 || p.store.Len("q2") != 1 {
		t.Fatalf("q1=%d q2=%d, want 1 1",
			p.store.Len("q1"), p.store.Len("q2"))
	}
}

// TestPublishNoRoute stores nothing when no queue matches.
func TestPublishNoRoute(t *testing.T) {
	p := setupPublisher()
	_, _ = p.exchanges.Declare(
		"amq.direct", ExchangeDirect,
		false, false, false, nil,
	)
	_, _ = p.queues.Declare("q1", false, false, false, nil, nil)

	msg := NewMessage([]byte("orphan"), Properties{})
	if err := p.Publish("amq.direct", "missing", msg, 1); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if p.store.Len("q1") != 0 {
		t.Fatalf("queue len = %d, want 0", p.store.Len("q1"))
	}
}

// TestPublishEndToEnd delivers to subscribed consumers.
func TestPublishEndToEnd(t *testing.T) {
	p := setupPublisher()
	_, _ = p.exchanges.Declare(
		"amq.direct", ExchangeDirect,
		false, false, false, nil,
	)
	_, _ = p.queues.Declare("q1", false, false, false, nil, nil)
	_, _ = p.bindings.Bind("amq.direct", "q1", "news", nil)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = p.consumers.Subscribe("c1", "q1", ch, false, false, nil)

	msg := NewMessage([]byte("e2e"), Properties{})
	if err := p.Publish("amq.direct", "news", msg, 1); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if p.store.Len("q1") != 1 {
		t.Fatalf("queue len = %d, want 1", p.store.Len("q1"))
	}
	if p.tracker.Count() != 1 {
		t.Fatalf("tracker count = %d, want 1", p.tracker.Count())
	}
}
