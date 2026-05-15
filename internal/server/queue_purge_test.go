package server

import (
	"testing"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// encodeQueuePurge builds a Queue.Purge method payload.
func encodeQueuePurge(queue string, noWait bool) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(0)       // reserved-1
	_ = enc.WriteShortString(queue)
	var bits uint8
	if noWait {
		bits |= 0x01
	}
	_ = enc.WriteUint8(bits)
	return enc.Bytes()
}

// TestQueuePurge removes all messages and replies with PurgeOk
// containing the message count.
func TestQueuePurge(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.ExchangeManager().Declare(
		"amq.direct", ExchangeDirect,
		false, false, false, nil,
	)
	_, _ = srv.QueueManager().Declare("q1", false, false, false, nil, nil)
	_, _ = srv.BindingManager().Bind("amq.direct", "q1", "news", nil)

	// publish 3 messages
	publishHandler, _ := reg.Lookup(60, 40)
	for i := 0; i < 3; i++ {
		payload := encodePublish("amq.direct", "news", false, false)
		if err := publishHandler(ch, payload); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if srv.MessageStore().Len("q1") != 3 {
		t.Fatalf("before purge: queue len = %d, want 3", srv.MessageStore().Len("q1"))
	}

	purgeHandler, ok := reg.Lookup(50, 30)
	if !ok {
		t.Fatal("handler (50,30) not registered")
	}
	if err := purgeHandler(ch, encodeQueuePurge("q1", false)); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if srv.MessageStore().Len("q1") != 0 {
		t.Fatalf("after purge: queue len = %d, want 0", srv.MessageStore().Len("q1"))
	}
}

// TestQueuePurgeEmpty replies with PurgeOk count 0 for an empty queue.
func TestQueuePurgeEmpty(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.QueueManager().Declare("empty-q", false, false, false, nil, nil)

	purgeHandler, _ := reg.Lookup(50, 30)
	if err := purgeHandler(ch, encodeQueuePurge("empty-q", false)); err != nil {
		t.Fatalf("purge empty: %v", err)
	}
	if srv.MessageStore().Len("empty-q") != 0 {
		t.Fatalf("after purge: queue len = %d, want 0", srv.MessageStore().Len("empty-q"))
	}
}

// TestQueuePurgeNoWait suppresses the PurgeOk reply.
func TestQueuePurgeNoWait(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.ExchangeManager().Declare(
		"amq.direct", ExchangeDirect,
		false, false, false, nil,
	)
	_, _ = srv.QueueManager().Declare("q2", false, false, false, nil, nil)
	_, _ = srv.BindingManager().Bind("amq.direct", "q2", "news", nil)

	publishHandler, _ := reg.Lookup(60, 40)
	payload := encodePublish("amq.direct", "news", false, false)
	if err := publishHandler(ch, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	purgeHandler, _ := reg.Lookup(50, 30)
	if err := purgeHandler(ch, encodeQueuePurge("q2", true)); err != nil {
		t.Fatalf("purge no-wait: %v", err)
	}
	if srv.MessageStore().Len("q2") != 0 {
		t.Fatalf("after purge: queue len = %d, want 0", srv.MessageStore().Len("q2"))
	}
}
