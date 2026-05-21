// advanced_queue_test.go tests TTL, priority queue, and dead-letter
// exchange behaviour.
package server

import (
	"fmt"
	"testing"
	"time"
)

// --- TTL tests -----------------------------------------------------------

func TestMessageTTL_Parse(t *testing.T) {
	msg := NewMessage([]byte("x"), Properties{Expiration: "1000"})
	if got := MessageTTL(msg); got != 1000*time.Millisecond {
		t.Fatalf("expected 1000ms, got %v", got)
	}
}

func TestMessageTTL_Missing(t *testing.T) {
	msg := NewMessage([]byte("x"), Properties{})
	if got := MessageTTL(msg); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}

func TestQueueTTL_Parse(t *testing.T) {
	args := map[string]interface{}{"x-message-ttl": 5000}
	if got := QueueTTL(args); got != 5000*time.Millisecond {
		t.Fatalf("expected 5000ms, got %v", got)
	}
}

func TestQueueTTL_Missing(t *testing.T) {
	if got := QueueTTL(nil); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}

func TestIsExpired_MessageTTL(t *testing.T) {
	msg := NewMessage([]byte("x"), Properties{Expiration: "1"})
	msg.SetEnqueuedAt(time.Now().Add(-2 * time.Millisecond))
	if !IsExpired(msg, nil) {
		t.Fatal("expected expired")
	}
}

func TestIsExpired_NotExpired(t *testing.T) {
	msg := NewMessage([]byte("x"), Properties{Expiration: "60000"})
	msg.SetEnqueuedAt(time.Now())
	if IsExpired(msg, nil) {
		t.Fatal("expected not expired")
	}
}

func TestIsExpired_QueueTTL(t *testing.T) {
	msg := NewMessage([]byte("x"), Properties{})
	args := map[string]interface{}{"x-message-ttl": 1}
	msg.SetEnqueuedAt(time.Now().Add(-2 * time.Millisecond))
	if !IsExpired(msg, args) {
		t.Fatal("expected expired")
	}
}

func TestIsExpired_NoTTL(t *testing.T) {
	msg := NewMessage([]byte("x"), Properties{})
	if IsExpired(msg, nil) {
		t.Fatal("expected not expired when no TTL")
	}
}

// --- Priority queue tests ------------------------------------------------

func TestMaxPriority_Extract(t *testing.T) {
	args := map[string]interface{}{"x-max-priority": 10}
	if got := MaxPriority(args); got != 10 {
		t.Fatalf("expected 10, got %d", got)
	}
}

func TestMaxPriority_Missing(t *testing.T) {
	if got := MaxPriority(nil); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestPriorityQueue_EnqueuePriority(t *testing.T) {
	store := NewMessageStore()
	msgLow := NewMessage([]byte("low"), Properties{Priority: 1})
	msgHigh := NewMessage([]byte("high"), Properties{Priority: 9})

	store.EnqueuePriority("q", msgLow)
	store.EnqueuePriority("q", msgHigh)

	first, ok := store.Dequeue("q")
	if !ok || string(first.Payload()) != "high" {
		t.Fatalf("expected high priority first, got %v", first)
	}
	second, ok := store.Dequeue("q")
	if !ok || string(second.Payload()) != "low" {
		t.Fatalf("expected low priority second, got %v", second)
	}
}

func TestPriorityQueue_MixedWithFIFO(t *testing.T) {
	store := NewMessageStore()
	msgA := NewMessage([]byte("a"), Properties{Priority: 5})
	msgB := NewMessage([]byte("b"), Properties{})

	store.EnqueuePriority("q", msgA)
	store.Enqueue("q", msgB) // FIFO tail

	first, ok := store.Dequeue("q")
	if !ok || string(first.Payload()) != "a" {
		t.Fatalf("expected priority first, got %v", first)
	}
}

// --- Dead-letter config tests --------------------------------------------

func TestGetDeadLetterConfig_Extract(t *testing.T) {
	args := map[string]interface{}{
		"x-dead-letter-exchange":   "dlx",
		"x-dead-letter-routing-key": "dlrk",
	}
	cfg := GetDeadLetterConfig(args)
	if cfg == nil {
		t.Fatal("expected config")
	}
	if cfg.Exchange != "dlx" || cfg.RoutingKey != "dlrk" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestGetDeadLetterConfig_Missing(t *testing.T) {
	if cfg := GetDeadLetterConfig(nil); cfg != nil {
		t.Fatalf("expected nil, got %+v", cfg)
	}
}

func TestMaxLength_Extract(t *testing.T) {
	args := map[string]interface{}{"x-max-length": 5}
	if got := MaxLength(args); got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
}

func TestMaxLength_Missing(t *testing.T) {
	if got := MaxLength(nil); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestShouldDeadLetter_True(t *testing.T) {
	args := map[string]interface{}{"x-dead-letter-exchange": "dlx"}
	if !ShouldDeadLetter(args, "rejected") {
		t.Fatal("expected true")
	}
}

func TestShouldDeadLetter_False(t *testing.T) {
	if ShouldDeadLetter(nil, "rejected") {
		t.Fatal("expected false")
	}
}

// --- Integration: max-length overflow + DLX ------------------------------

func TestPublisher_MaxLengthOverflow_DeadLetter(t *testing.T) {
	ex := NewExchangeManager()
	qm := NewQueueManager()
	bm := NewBindingManager()
	store := NewMessageStore()
	cm := NewConsumerManager()
	tracker := NewDeliveryTracker(store)
	pub := NewPublisher(ex, qm, bm, nil, store, cm, tracker)

	// Create exchange.
	_, _ = ex.Declare("ex", ExchangeDirect, false, false, false, nil)

	// Create main queue with max-length=2 and DLX.
	_, _ = qm.Declare("main", false, false, false,
		map[string]interface{}{
			"x-max-length":              2,
			"x-dead-letter-exchange":    "dlx",
			"x-dead-letter-routing-key": "dlrk",
		}, nil)

	// Bind main queue to exchange.
	_, _ = bm.Bind("ex", "main", "rk", nil)

	// Create DLX exchange and bound queue.
	_, _ = ex.Declare("dlx", ExchangeDirect, false, false, false, nil)
	_, _ = qm.Declare("dlq", false, false, false, nil, nil)
	_, _ = bm.Bind("dlx", "dlq", "dlrk", nil)

	// Publish 3 messages.
	for i := 1; i <= 3; i++ {
		msg := NewMessage([]byte(fmt.Sprintf("msg%d", i)), Properties{})
		msg.SetRoutingMeta("ex", "rk")
		_, _ = pub.Publish("ex", "rk", msg, 1)
	}

	// Main queue should have exactly 2 messages.
	if store.Len("main") != 2 {
		t.Fatalf("expected main queue length 2, got %d", store.Len("main"))
	}

	// DLQ should have 1 dead-lettered message (the oldest overflowed).
	if store.Len("dlq") != 1 {
		t.Fatalf("expected dlq length 1, got %d", store.Len("dlq"))
	}

	// Verify the dead-lettered message is the first one.
	dlMsg, ok := store.Dequeue("dlq")
	if !ok || string(dlMsg.Payload()) != "msg1" {
		t.Fatalf("expected dlq first message 'msg1', got %v", dlMsg)
	}
}

// --- Integration: TTL skip on pull -----------------------------------------

func TestPullConsumer_Get_SkipsExpired(t *testing.T) {
	store := NewMessageStore()
	tracker := NewDeliveryTracker(store)
	qm := NewQueueManager()
	q, _ := qm.Declare("ttlq", false, false, false,
		map[string]interface{}{"x-message-ttl": 1}, nil)
	_ = q // queue declared

	msg := NewMessage([]byte("expired"), Properties{})
	msg.SetEnqueuedAt(time.Now().Add(-2 * time.Millisecond))
	store.Enqueue("ttlq", msg)

	pc := NewPullConsumerWithQueueManager(store, tracker, qm)
	got, ok := pc.Get("ttlq", true, 1)
	if ok {
		t.Fatalf("expected no message (expired), got %v", got)
	}
}

func TestPullConsumer_Get_DeliversNonExpired(t *testing.T) {
	store := NewMessageStore()
	tracker := NewDeliveryTracker(store)
	qm := NewQueueManager()
	_, _ = qm.Declare("ttlq", false, false, false,
		map[string]interface{}{"x-message-ttl": 60000}, nil)

	msg := NewMessage([]byte("fresh"), Properties{})
	msg.SetEnqueuedAt(time.Now())
	store.Enqueue("ttlq", msg)

	pc := NewPullConsumerWithQueueManager(store, tracker, qm)
	got, ok := pc.Get("ttlq", true, 1)
	if !ok || string(got.Payload()) != "fresh" {
		t.Fatalf("expected fresh message, got %v, ok=%v", got, ok)
	}
}

// --- Integration: priority enqueue via Publisher --------------------------

func TestPublisher_PriorityEnqueue(t *testing.T) {
	ex := NewExchangeManager()
	qm := NewQueueManager()
	bm := NewBindingManager()
	store := NewMessageStore()
	cm := NewConsumerManager()
	tracker := NewDeliveryTracker(store)
	pub := NewPublisher(ex, qm, bm, nil, store, cm, tracker)

	_, _ = ex.Declare("ex", ExchangeDirect, false, false, false, nil)
	_, _ = qm.Declare("priq", false, false, false,
		map[string]interface{}{"x-max-priority": 10}, nil)
	_, _ = bm.Bind("ex", "priq", "rk", nil)

	msgLow := NewMessage([]byte("low"), Properties{Priority: 1})
	msgLow.SetRoutingMeta("ex", "rk")
	msgHigh := NewMessage([]byte("high"), Properties{Priority: 9})
	msgHigh.SetRoutingMeta("ex", "rk")

	_, _ = pub.Publish("ex", "rk", msgLow, 1)
	_, _ = pub.Publish("ex", "rk", msgHigh, 1)

	first, _ := store.Dequeue("priq")
	if string(first.Payload()) != "high" {
		t.Fatalf("expected high priority first, got %s", first.Payload())
	}
	second, _ := store.Dequeue("priq")
	if string(second.Payload()) != "low" {
		t.Fatalf("expected low priority second, got %s", second.Payload())
	}
}

// --- Integration: DLX on reject ------------------------------------------

func TestReject_DeadLetters(t *testing.T) {
	store := NewMessageStore()
	tracker := NewDeliveryTracker(store)
	ex := NewExchangeManager()
	qm := NewQueueManager()
	bm := NewBindingManager()
	cm := NewConsumerManager()
	pub := NewPublisher(ex, qm, bm, nil, store, cm, tracker)
	_ = pub

	// Create source queue with DLX.
	_, _ = qm.Declare("src", false, false, false,
		map[string]interface{}{
			"x-dead-letter-exchange":    "dlx",
			"x-dead-letter-routing-key": "dlrk",
		}, nil)

	// DLX + DLQ.
	_, _ = ex.Declare("dlx", ExchangeDirect, false, false, false, nil)
	_, _ = qm.Declare("dlq", false, false, false, nil, nil)
	_, _ = bm.Bind("dlx", "dlq", "dlrk", nil)

	// Enqueue and track a message.
	msg := NewMessage([]byte("reject-me"), Properties{})
	store.Enqueue("src", msg)
	tracker.Record(1, msg, "src", 1)

	// Simulate reject-without-requeue + dead-letter routing.
	d := tracker.GetDelivery(1, 1)
	_ = tracker.Reject(1, 1, false)
	if d != nil {
		pub.DeadLetter(d.Message(), map[string]interface{}{
			"x-dead-letter-exchange":    "dlx",
			"x-dead-letter-routing-key": "dlrk",
		})
	}

	// DLQ should receive the dead-lettered message.
	if store.Len("dlq") != 1 {
		t.Fatalf("expected dlq length 1 after reject, got %d", store.Len("dlq"))
	}
}
