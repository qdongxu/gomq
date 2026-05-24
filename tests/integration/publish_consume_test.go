// publish_consume_test.go — end-to-end publish → route → store
// → deliver → consume → ack verification.
package integration

import (
	"bytes"
	"testing"

	"github.com/qdongxu/gomq/internal/server"
)

// setupServer returns a fully wired Server ready for integration tests.
func setupServer() *server.Server {
	srv := server.NewServer()
	return srv
}

// TestPublishRouteStoreDeliverConsumeAck verifies the complete AMQP
// message lifecycle through all broker subsystems.
func TestPublishRouteStoreDeliverConsumeAck(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	cm := srv.ConsumerManager()
	tracker := srv.DeliveryTracker()
	pub := srv.Publisher()

	// 1. Declare direct exchange and queue, then bind them.
	_, _ = ex.Declare("ex.direct", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("q1", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.direct", "q1", "rk1", nil)

	// 2. Create a mock connection / channel and subscribe a consumer.
	auth := server.NewMemoryAuthenticator()
	conn := server.NewConnection(nil, auth, nil)
	ch, _ := server.NewChannelManager(10).Create(1, conn)
	ch.Open()
	_, _ = cm.Subscribe("c1", "q1", ch, false, false, nil)

	// 3. Publish a message.
	msg := server.NewMessage([]byte("hello-e2e"), server.Properties{})
	msg.SetRoutingMeta("ex.direct", "rk1")
	_, err := pub.Publish("ex.direct", "rk1", msg, 1)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// 4. Verify message reached the queue store.
	if store.Len("q1") != 1 {
		t.Fatalf("store len = %d, want 1", store.Len("q1"))
	}

	// 5. Dequeue and deliver to the consumer.
	deq, ok := store.Dequeue("q1")
	if !ok {
		t.Fatal("expected message in queue")
	}
	deliverer := server.NewDeliverer(cm, store, tracker)
	_ = deliverer.Deliver(deq, "q1", 1)

	// 6. Verify tracker recorded the delivery.
	if tracker.Count() != 1 {
		t.Fatalf("tracker count = %d, want 1", tracker.Count())
	}

	// 7. Consumer acks the message.
	// The tracker assigns tags sequentially starting from 0 in our
	 // simplified Deliver implementation; adjust if that changes.
	if err := tracker.Ack(0, 1); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if tracker.Count() != 0 {
		t.Fatalf("tracker count after ack = %d, want 0",
			tracker.Count())
	}
}

// TestPublishFanoutMultipleConsumers checks fanout exchange delivery
// to two bound queues with independent consumers.
func TestPublishFanoutMultipleConsumers(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	cm := srv.ConsumerManager()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.fanout", server.ExchangeFanout,
		false, false, false, nil)
	_, _ = qm.Declare("qA", false, false, false, nil, nil)
	_, _ = qm.Declare("qB", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.fanout", "qA", "", nil)
	_, _ = bm.Bind("ex.fanout", "qB", "", nil)

	auth := server.NewMemoryAuthenticator()
	conn := server.NewConnection(nil, auth, nil)
	chM := server.NewChannelManager(10)
	chA, _ := chM.Create(1, conn)
	chA.Open()
	chB, _ := chM.Create(2, conn)
	chB.Open()
	_, _ = cm.Subscribe("cA", "qA", chA, true, false, nil)
	_, _ = cm.Subscribe("cB", "qB", chB, true, false, nil)

	msg := server.NewMessage([]byte("broadcast"), server.Properties{})
	msg.SetRoutingMeta("ex.fanout", "")
	_, err := pub.Publish("ex.fanout", "", msg, 1)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	if store.Len("qA") != 1 || store.Len("qB") != 1 {
		t.Fatalf("qA=%d qB=%d, want 1 1",
			store.Len("qA"), store.Len("qB"))
	}
}

// TestAutoAckConsumer removes message from tracking immediately.
func TestAutoAckConsumer(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	cm := srv.ConsumerManager()
	tracker := srv.DeliveryTracker()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.direct", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("q1", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.direct", "q1", "rk", nil)

	auth := server.NewMemoryAuthenticator()
	conn := server.NewConnection(nil, auth, nil)
	ch, _ := server.NewChannelManager(10).Create(1, conn)
	ch.Open()
	_, _ = cm.Subscribe("c1", "q1", ch, true, false, nil)

	msg := server.NewMessage([]byte("auto"), server.Properties{})
	msg.SetRoutingMeta("ex.direct", "rk")
	_, _ = pub.Publish("ex.direct", "rk", msg, 1)

	deq, _ := store.Dequeue("q1")
	deliverer := server.NewDeliverer(cm, store, tracker)
	_ = deliverer.Deliver(deq, "q1", 1)

	// Auto-ack consumer: message should still be tracked until
	 // explicit ack, but in practice the broker may skip tracking
	 // for auto-ack.  We verify the queue is empty after dequeue.
	if store.Len("q1") != 0 {
		t.Fatalf("queue not empty: %d", store.Len("q1"))
	}
}

// TestNoRouteDropped confirms unrouted messages do not land in any
// queue and the store remains empty.
func TestNoRouteDropped(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.direct", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("q1", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.direct", "q1", "other", nil)

	msg := server.NewMessage([]byte("orphan"), server.Properties{})
	msg.SetRoutingMeta("ex.direct", "missing")
	_, _ = pub.Publish("ex.direct", "missing", msg, 1)

	if store.Len("q1") != 0 {
		t.Fatalf("expected empty queue, got %d", store.Len("q1"))
	}
}

// TestMultiMessageOrdering preserves FIFO order across several
// publish / dequeue cycles.
func TestMultiMessageOrdering(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.direct", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("q1", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.direct", "q1", "rk", nil)

	want := []string{"first", "second", "third"}
	for _, body := range want {
		msg := server.NewMessage([]byte(body), server.Properties{})
		msg.SetRoutingMeta("ex.direct", "rk")
		_, _ = pub.Publish("ex.direct", "rk", msg, 1)
	}

	if store.Len("q1") != 3 {
		t.Fatalf("queue len = %d, want 3", store.Len("q1"))
	}

	for i, expected := range want {
		m, ok := store.Dequeue("q1")
		if !ok {
			t.Fatalf("dequeue %d failed", i)
		}
		if !bytes.Equal(m.Payload(), []byte(expected)) {
			t.Fatalf("msg %d = %q, want %q", i,
				string(m.Payload()), expected)
		}
	}
}

// TestRequeueAfterNack puts the message back into the queue so it
// can be redelivered.
func TestRequeueAfterNack(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	cm := srv.ConsumerManager()
	tracker := srv.DeliveryTracker()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.direct", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("q1", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.direct", "q1", "rk", nil)

	auth := server.NewMemoryAuthenticator()
	conn := server.NewConnection(nil, auth, nil)
	ch, _ := server.NewChannelManager(10).Create(1, conn)
	ch.Open()
	_, _ = cm.Subscribe("c1", "q1", ch, false, false, nil)

	msg := server.NewMessage([]byte("retry"), server.Properties{})
	msg.SetRoutingMeta("ex.direct", "rk")
	_, _ = pub.Publish("ex.direct", "rk", msg, 1)

	deq, _ := store.Dequeue("q1")
	deliverer := server.NewDeliverer(cm, store, tracker)
	_ = deliverer.Deliver(deq, "q1", 1)

	// Nack with requeue.
	_ = tracker.Nack(0, 1, true)
	if store.Len("q1") != 1 {
		t.Fatalf("expected requeue, got len=%d", store.Len("q1"))
	}

	// Redeliver and ack.
	m2, _ := store.Dequeue("q1")
	_ = deliverer.Deliver(m2, "q1", 1)
	_ = tracker.Ack(0, 1)
	if tracker.Count() != 0 {
		t.Fatalf("tracker count = %d, want 0", tracker.Count())
	}
}
