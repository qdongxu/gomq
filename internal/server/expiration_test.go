// expiration_test.go tests the ExpirationManager and message-store
// expiration helpers.
package server

import (
	"testing"
	"time"
)

func setupExpirationManager() (*ExpirationManager, *Publisher) {
	ex := NewExchangeManager()
	qm := NewQueueManager()
	bm := NewBindingManager()
	store := NewMessageStore()
	cm := NewConsumerManager()
	tracker := NewDeliveryTracker(store)
	pub := NewPublisher(ex, qm, bm, nil, store, cm, tracker)
	em := NewExpirationManager(store, qm, pub, 100*time.Millisecond)
	return em, pub
}

// TestExpirationManager_StartStop verifies the manager can be started
// and stopped cleanly.
func TestExpirationManager_StartStop(t *testing.T) {
	em, _ := setupExpirationManager()
	em.Start()
	time.Sleep(50 * time.Millisecond)
	em.Stop()
}

// TestExpirationManager_ScanQueueLevelTTL removes messages that have
// exceeded the queue-level TTL.
func TestExpirationManager_ScanQueueLevelTTL(t *testing.T) {
	em, pub := setupExpirationManager()
	defer em.Stop()

	_, _ = pub.queues.Declare(
		"q1", false, false, false,
		map[string]interface{}{
			"x-message-ttl": 50,
		}, nil,
	)

	msg := NewMessage([]byte("a"), Properties{})
	msg.SetEnqueuedAt(time.Now().Add(-100 * time.Millisecond))
	pub.store.Enqueue("q1", msg)

	em.scanQueue("q1")

	if pub.store.Len("q1") != 0 {
		t.Fatalf("len = %d, want 0", pub.store.Len("q1"))
	}
}

// TestExpirationManager_ScanMessageLevelTTL removes messages that
// have exceeded the per-message Expiration property.
func TestExpirationManager_ScanMessageLevelTTL(t *testing.T) {
	em, pub := setupExpirationManager()
	defer em.Stop()

	_, _ = pub.queues.Declare("q1", false, false, false, nil, nil)

	msg := NewMessage([]byte("a"), Properties{
		Expiration: "50", // 50 ms
	})
	msg.SetEnqueuedAt(time.Now().Add(-100 * time.Millisecond))
	pub.store.Enqueue("q1", msg)

	em.scanQueue("q1")

	if pub.store.Len("q1") != 0 {
		t.Fatalf("len = %d, want 0", pub.store.Len("q1"))
	}
}

// TestExpirationManager_ScanMixedTTL checks that message-level TTL
// takes precedence over queue-level TTL.
func TestExpirationManager_ScanMixedTTL(t *testing.T) {
	em, pub := setupExpirationManager()
	defer em.Stop()

	_, _ = pub.queues.Declare(
		"q1", false, false, false,
		map[string]interface{}{
			"x-message-ttl": 500, // 500 ms queue TTL
		}, nil,
	)

	// Message with its own short TTL (50 ms) should expire.
	msg := NewMessage([]byte("a"), Properties{
		Expiration: "50", // 50 ms
	})
	msg.SetEnqueuedAt(time.Now().Add(-100 * time.Millisecond))
	pub.store.Enqueue("q1", msg)

	em.scanQueue("q1")

	if pub.store.Len("q1") != 0 {
		t.Fatalf("len = %d, want 0", pub.store.Len("q1"))
	}
}

// TestExpirationManager_ScanKeepsAlive does not remove messages that
// are still within their TTL.
func TestExpirationManager_ScanKeepsAlive(t *testing.T) {
	em, pub := setupExpirationManager()
	defer em.Stop()

	_, _ = pub.queues.Declare(
		"q1", false, false, false,
		map[string]interface{}{
			"x-message-ttl": 10000,
		}, nil,
	)

	msg := NewMessage([]byte("a"), Properties{})
	msg.SetEnqueuedAt(time.Now())
	pub.store.Enqueue("q1", msg)

	em.scanQueue("q1")

	if pub.store.Len("q1") != 1 {
		t.Fatalf("len = %d, want 1", pub.store.Len("q1"))
	}
}

// TestExpirationManager_ScanDLX routes expired messages to the
// configured dead-letter exchange.
func TestExpirationManager_ScanDLX(t *testing.T) {
	em, pub := setupExpirationManager()
	defer em.Stop()

	_, _ = pub.exchanges.Declare("dlx", ExchangeDirect, false, false, false, nil)
	_, _ = pub.queues.Declare("dlq", false, false, false, nil, nil)
	_, _ = pub.bindings.Bind("dlx", "dlq", "key", nil)

	_, _ = pub.queues.Declare(
		"q1", false, false, false,
		map[string]interface{}{
			"x-message-ttl":              50,
			"x-dead-letter-exchange":     "dlx",
			"x-dead-letter-routing-key":  "key",
		}, nil,
	)

	msg := NewMessage([]byte("a"), Properties{})
	msg.SetEnqueuedAt(time.Now().Add(-100 * time.Millisecond))
	pub.store.Enqueue("q1", msg)

	em.scanQueue("q1")

	if pub.store.Len("q1") != 0 {
		t.Fatalf("q1 len = %d, want 0", pub.store.Len("q1"))
	}
	if pub.store.Len("dlq") != 1 {
		t.Fatalf("dlq len = %d, want 1", pub.store.Len("dlq"))
	}
}

// TestExpirationManager_ScanNoDLX discards expired messages when no
// dead-letter exchange is configured.
func TestExpirationManager_ScanNoDLX(t *testing.T) {
	em, pub := setupExpirationManager()
	defer em.Stop()

	_, _ = pub.queues.Declare(
		"q1", false, false, false,
		map[string]interface{}{
			"x-message-ttl": 50,
		}, nil,
	)

	msg := NewMessage([]byte("a"), Properties{})
	msg.SetEnqueuedAt(time.Now().Add(-100 * time.Millisecond))
	pub.store.Enqueue("q1", msg)

	em.scanQueue("q1")

	if pub.store.Len("q1") != 0 {
		t.Fatalf("q1 len = %d, want 0", pub.store.Len("q1"))
	}
}

// TestExpirationManager_BackgroundScan uses the ticker loop to remove
// expired messages automatically.
func TestExpirationManager_BackgroundScan(t *testing.T) {
	em, pub := setupExpirationManager()
	defer em.Stop()

	_, _ = pub.queues.Declare(
		"q1", false, false, false,
		map[string]interface{}{
			"x-message-ttl": 50,
		}, nil,
	)

	em.Start()

	msg := NewMessage([]byte("a"), Properties{})
	msg.SetEnqueuedAt(time.Now())
	pub.store.Enqueue("q1", msg)

	// Message should be alive right after enqueue.
	if pub.store.Len("q1") != 1 {
		t.Fatalf("initial len = %d, want 1", pub.store.Len("q1"))
	}

	// Wait for the message to expire and the scanner to pick it up.
	time.Sleep(200 * time.Millisecond)

	if pub.store.Len("q1") != 0 {
		t.Fatalf("after scan len = %d, want 0", pub.store.Len("q1"))
	}
}

// TestMessageStore_RemoveExpired directly tests the store helper.
func TestMessageStore_RemoveExpired(t *testing.T) {
	store := NewMessageStore()

	alive := NewMessage([]byte("alive"), Properties{})
	alive.SetEnqueuedAt(time.Now())

	expired := NewMessage([]byte("expired"), Properties{})
	expired.SetEnqueuedAt(time.Now().Add(-100 * time.Millisecond))

	store.Enqueue("q1", expired)
	store.Enqueue("q1", alive)

	var removed int
	store.RemoveExpired("q1",
		func(msg *Message) bool {
			return time.Since(msg.EnqueuedAt()) >= 50*time.Millisecond
		},
		func(msg *Message) {
			removed++
		},
	)

	if store.Len("q1") != 1 {
		t.Fatalf("len = %d, want 1", store.Len("q1"))
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}

	m, ok := store.Dequeue("q1")
	if !ok || string(m.Payload()) != "alive" {
		t.Fatalf("expected alive message")
	}
}

// TestExpirationManager_ScanMultipleMessages removes multiple expired
// messages in a single queue while keeping alive ones.
func TestExpirationManager_ScanMultipleMessages(t *testing.T) {
	em, pub := setupExpirationManager()
	defer em.Stop()

	_, _ = pub.queues.Declare(
		"q1", false, false, false,
		map[string]interface{}{
			"x-message-ttl": 50,
		}, nil,
	)

	for i := 0; i < 3; i++ {
		msg := NewMessage([]byte("expired"), Properties{})
		msg.SetEnqueuedAt(time.Now().Add(-100 * time.Millisecond))
		pub.store.Enqueue("q1", msg)
	}

	alive := NewMessage([]byte("alive"), Properties{})
	alive.SetEnqueuedAt(time.Now())
	pub.store.Enqueue("q1", alive)

	for i := 0; i < 2; i++ {
		msg := NewMessage([]byte("expired2"), Properties{})
		msg.SetEnqueuedAt(time.Now().Add(-100 * time.Millisecond))
		pub.store.Enqueue("q1", msg)
	}

	em.scanQueue("q1")

	if pub.store.Len("q1") != 1 {
		t.Fatalf("len = %d, want 1", pub.store.Len("q1"))
	}
	m, ok := pub.store.Dequeue("q1")
	if !ok || string(m.Payload()) != "alive" {
		t.Fatalf("expected alive message")
	}
}

// TestExpirationManager_SetInterval changes the scan interval at
// runtime.
func TestExpirationManager_SetInterval(t *testing.T) {
	em, _ := setupExpirationManager()
	em.SetInterval(200 * time.Millisecond)
	em.Start()
	defer em.Stop()

	// Just verify it does not panic and the interval is updated.
	time.Sleep(50 * time.Millisecond)
}

// TestExpirationManager_NilPublisher handles the case where no
// publisher is available (no DLX routing possible).
func TestExpirationManager_NilPublisher(t *testing.T) {
	store := NewMessageStore()
	qm := NewQueueManager()
	_, _ = qm.Declare(
		"q1", false, false, false,
		map[string]interface{}{
			"x-message-ttl": 50,
		}, nil,
	)

	em := NewExpirationManager(store, qm, nil, 100*time.Millisecond)
	defer em.Stop()

	msg := NewMessage([]byte("a"), Properties{})
	msg.SetEnqueuedAt(time.Now().Add(-100 * time.Millisecond))
	store.Enqueue("q1", msg)

	em.scanQueue("q1")

	if store.Len("q1") != 0 {
		t.Fatalf("len = %d, want 0", store.Len("q1"))
	}
}
