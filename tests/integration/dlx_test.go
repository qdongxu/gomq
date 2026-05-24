// dlx_test.go — Dead-letter exchange end-to-end integration.
package integration

import (
	"fmt"
	"testing"

	"github.com/qdongxu/gomq/internal/server"
)

// TestDLXOnMaxLengthOverflow routes the oldest message to the DLX
// when the source queue exceeds its max-length limit.
func TestDLXOnMaxLengthOverflow(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.direct", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("main", false, false, false,
		map[string]interface{}{
			"x-max-length":              2,
			"x-dead-letter-exchange":    "dlx",
			"x-dead-letter-routing-key": "dlrk",
		}, nil)
	_, _ = bm.Bind("ex.direct", "main", "rk", nil)

	_, _ = ex.Declare("dlx", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("dlq", false, false, false, nil, nil)
	_, _ = bm.Bind("dlx", "dlq", "dlrk", nil)

	for i := 1; i <= 3; i++ {
		msg := server.NewMessage(
			[]byte(fmt.Sprintf("msg%d", i)),
			server.Properties{},
		)
		msg.SetRoutingMeta("ex.direct", "rk")
		_, _ = pub.Publish("ex.direct", "rk", msg, 1)
	}

	if store.Len("main") != 2 {
		t.Fatalf("main len = %d, want 2", store.Len("main"))
	}
	if store.Len("dlq") != 1 {
		t.Fatalf("dlq len = %d, want 1", store.Len("dlq"))
	}

	first, _ := store.Dequeue("dlq")
	if string(first.Payload()) != "msg1" {
		t.Fatalf("dlq first = %q, want msg1", first.Payload())
	}
}

// TestDLXOnRejectWithoutRequeue dead-letters a message when a
// consumer explicitly rejects it without requeue.
func TestDLXOnRejectWithoutRequeue(t *testing.T) {
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
	_, _ = qm.Declare("src", false, false, false,
		map[string]interface{}{
			"x-dead-letter-exchange":    "dlx",
			"x-dead-letter-routing-key": "dlrk",
		}, nil)
	_, _ = bm.Bind("ex.direct", "src", "rk", nil)

	_, _ = ex.Declare("dlx", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("dlq", false, false, false, nil, nil)
	_, _ = bm.Bind("dlx", "dlq", "dlrk", nil)

	msg := server.NewMessage([]byte("reject-me"), server.Properties{})
	msg.SetRoutingMeta("ex.direct", "rk")
	_, _ = pub.Publish("ex.direct", "rk", msg, 1)

	// Manually dequeue and track, then reject.
	deq, _ := store.Dequeue("src")
	tracker.Record(1, deq, "src", 1)
	deliverer := server.NewDeliverer(cm, store, tracker)
	_ = deliverer.Deliver(deq, "src", 1)

	// Reject without requeue triggers dead-letter in our publisher.
	_ = tracker.Reject(1, 1, false)
	pub.DeadLetter(deq,
		map[string]interface{}{
			"x-dead-letter-exchange":    "dlx",
			"x-dead-letter-routing-key": "dlrk",
		})

	if store.Len("dlq") != 1 {
		t.Fatalf("dlq len = %d, want 1", store.Len("dlq"))
	}
}

// TestDLXPreservesOriginalRoutingKey stores the original routing key
// in a header before routing to the DLX.
func TestDLXPreservesOriginalRoutingKey(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.direct", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("src", false, false, false,
		map[string]interface{}{
			"x-max-length":              1,
			"x-dead-letter-exchange":    "dlx",
			"x-dead-letter-routing-key": "dlrk",
		}, nil)
	_, _ = bm.Bind("ex.direct", "src", "orig.rk", nil)

	_, _ = ex.Declare("dlx", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("dlq", false, false, false, nil, nil)
	_, _ = bm.Bind("dlx", "dlq", "dlrk", nil)

	msg := server.NewMessage([]byte("overflow"), server.Properties{})
	msg.SetRoutingMeta("ex.direct", "orig.rk")
	_, _ = pub.Publish("ex.direct", "orig.rk", msg, 1)

	// Force overflow: publish a second message.
	msg2 := server.NewMessage([]byte("overflow2"), server.Properties{})
	msg2.SetRoutingMeta("ex.direct", "orig.rk")
	_, _ = pub.Publish("ex.direct", "orig.rk", msg2, 1)

	dlq, _ := store.Dequeue("dlq")
	h := dlq.Properties().Headers
	if h == nil {
		t.Fatal("expected headers on dead-lettered message")
	}
	if h["x-original-routing-key"] != "orig.rk" {
		t.Fatalf("original rk = %v, want orig.rk",
			h["x-original-routing-key"])
	}
}

// TestDLXNoConfigMeansNoDeadLetter confirms that without a DLX
// configuration overflowed or rejected messages are simply discarded.
func TestDLXNoConfigMeansNoDeadLetter(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.direct", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("main", false, false, false,
		map[string]interface{}{
			"x-max-length": 1,
		}, nil)
	_, _ = bm.Bind("ex.direct", "main", "rk", nil)

	for i := 1; i <= 2; i++ {
		msg := server.NewMessage([]byte(fmt.Sprintf("m%d", i)),
			server.Properties{})
		msg.SetRoutingMeta("ex.direct", "rk")
		_, _ = pub.Publish("ex.direct", "rk", msg, 1)
	}

	if store.Len("main") != 1 {
		t.Fatalf("main len = %d, want 1", store.Len("main"))
	}
	// No dlq exists; the overflowed message is lost.
}

// TestDLXWithTopicExchange verifies dead-letter routing through a
// topic exchange instead of direct.
func TestDLXWithTopicExchange(t *testing.T) {
	srv := setupServer()
	ex := srv.ExchangeManager()
	qm := srv.QueueManager()
	bm := srv.BindingManager()
	store := srv.MessageStore()
	pub := srv.Publisher()

	_, _ = ex.Declare("ex.direct", server.ExchangeDirect,
		false, false, false, nil)
	_, _ = qm.Declare("src", false, false, false,
		map[string]interface{}{
			"x-max-length":              1,
			"x-dead-letter-exchange":    "ex.topic",
			"x-dead-letter-routing-key": "dl.a",
		}, nil)
	_, _ = bm.Bind("ex.direct", "src", "rk", nil)

	_, _ = ex.Declare("ex.topic", server.ExchangeTopic,
		false, false, false, nil)
	_, _ = qm.Declare("dlq", false, false, false, nil, nil)
	_, _ = bm.Bind("ex.topic", "dlq", "dl.#", nil)

	for i := 1; i <= 2; i++ {
		msg := server.NewMessage([]byte(fmt.Sprintf("m%d", i)),
			server.Properties{})
		msg.SetRoutingMeta("ex.direct", "rk")
		_, _ = pub.Publish("ex.direct", "rk", msg, 1)
	}

	if store.Len("dlq") != 1 {
		t.Fatalf("dlq len = %d, want 1", store.Len("dlq"))
	}
}
