// expiration.go implements background expiration scanning and cleanup.
package server

import (
	"sync"
	"time"
)

// ExpirationManager periodically scans all queues and removes expired
// messages. When a dead-letter exchange is configured, expired messages
// are routed to it before being discarded.
type ExpirationManager struct {
	store     *MessageStore
	queueMgr  *QueueManager
	publisher *Publisher
	interval  time.Duration
	stopCh    chan struct{}
	mu        sync.Mutex
	running   bool
	wg        sync.WaitGroup
}

// NewExpirationManager creates an expiration manager with the given
// scan interval. A non-positive interval defaults to one minute.
func NewExpirationManager(
	store *MessageStore,
	queueMgr *QueueManager,
	publisher *Publisher,
	interval time.Duration,
) *ExpirationManager {
	if interval <= 0 {
		interval = 1 * time.Minute
	}
	return &ExpirationManager{
		store:     store,
		queueMgr:  queueMgr,
		publisher: publisher,
		interval:  interval,
		stopCh:    make(chan struct{}),
	}
}

// Start begins the background scan loop.
func (em *ExpirationManager) Start() {
	em.mu.Lock()
	defer em.mu.Unlock()
	if em.running {
		return
	}
	em.running = true
	em.wg.Add(1)
	go em.loop()
}

// Stop halts the background scan loop and waits for it to finish.
func (em *ExpirationManager) Stop() {
	em.mu.Lock()
	if !em.running {
		em.mu.Unlock()
		return
	}
	em.running = false
	close(em.stopCh)
	em.mu.Unlock()
	em.wg.Wait()
}

// SetInterval changes the scan interval at runtime. The new interval
// takes effect on the next tick.
func (em *ExpirationManager) SetInterval(d time.Duration) {
	em.mu.Lock()
	defer em.mu.Unlock()
	em.interval = d
}

func (em *ExpirationManager) loop() {
	defer em.wg.Done()
	ticker := time.NewTicker(em.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			em.scanAll()
		case <-em.stopCh:
			return
		}
	}
}

// scanAll iterates over every queue and removes expired messages.
func (em *ExpirationManager) scanAll() {
	for _, queueName := range em.store.AllQueues() {
		em.scanQueue(queueName)
	}
}

// scanQueue removes expired messages from a single queue. When DLX is
// configured, expired messages are routed there before being discarded.
func (em *ExpirationManager) scanQueue(queueName string) {
	var queueArgs map[string]interface{}
	if em.queueMgr != nil {
		if q, ok := em.queueMgr.Get(queueName); ok {
			queueArgs = q.Args
		}
	}

	em.store.RemoveExpired(queueName,
		func(msg *Message) bool {
			return IsExpired(msg, queueArgs)
		},
		func(msg *Message) {
			if em.publisher != nil &&
				ShouldDeadLetter(queueArgs, "expired") {
				em.publisher.DeadLetter(msg, queueArgs)
			}
		},
	)
}
