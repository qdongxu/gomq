// delivery_tracker.go tracks in-flight deliveries and handles ack/nack/reject.
package server

import (
	"fmt"
	"sync"
)

// DeliveryTracker records messages sent to consumers that have not
// yet been acknowledged.
type DeliveryTracker struct {
	byChannel map[uint16]map[uint64]*TrackedDelivery
	store     *MessageStore
	mu        sync.RWMutex
}

// NewDeliveryTracker creates a tracker backed by a message store.
func NewDeliveryTracker(store *MessageStore) *DeliveryTracker {
	return &DeliveryTracker{
		byChannel: make(map[uint16]map[uint64]*TrackedDelivery),
		store:     store,
	}
}

// Record registers a delivery for later acknowledgement.
func (t *DeliveryTracker) Record(
	deliveryTag uint64,
	msg *Message,
	queueName string,
	channelID uint16,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.byChannel[channelID] == nil {
		t.byChannel[channelID] = make(map[uint64]*TrackedDelivery)
	}
	t.byChannel[channelID][deliveryTag] = NewTrackedDelivery(
		deliveryTag, msg, queueName, channelID,
	)
}

// Ack removes a delivery from tracking.
func (t *DeliveryTracker) Ack(deliveryTag uint64, channelID uint16) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	chMap := t.byChannel[channelID]
	if chMap == nil {
		return fmt.Errorf("channel %d has no deliveries", channelID)
	}
	if _, ok := chMap[deliveryTag]; !ok {
		return fmt.Errorf("delivery tag %d not found", deliveryTag)
	}
	delete(chMap, deliveryTag)
	return nil
}

// Nack negatively acknowledges a delivery, optionally requeueing.
func (t *DeliveryTracker) Nack(
	deliveryTag uint64,
	channelID uint16,
	requeue bool,
) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	chMap := t.byChannel[channelID]
	if chMap == nil {
		return fmt.Errorf("channel %d has no deliveries", channelID)
	}
	d, ok := chMap[deliveryTag]
	if !ok {
		return fmt.Errorf("delivery tag %d not found", deliveryTag)
	}
	delete(chMap, deliveryTag)
	if requeue {
		t.store.Enqueue(d.queueName, d.msg)
	}
	return nil
}

// Reject rejects a delivery, optionally requeueing.
func (t *DeliveryTracker) Reject(
	deliveryTag uint64,
	channelID uint16,
	requeue bool,
) error {
	return t.Nack(deliveryTag, channelID, requeue)
}

// RecoverAll recovers all unacknowledged deliveries for a channel.
// When requeue is true, messages are re-enqueued to their original
// queues. Returns the number of recovered deliveries.
func (t *DeliveryTracker) RecoverAll(
	channelID uint16,
	requeue bool,
) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	chMap := t.byChannel[channelID]
	if chMap == nil {
		return 0
	}

	count := 0
	if requeue {
		for _, d := range chMap {
			t.store.Enqueue(d.queueName, d.msg)
			count++
		}
	} else {
		count = len(chMap)
	}

	delete(t.byChannel, channelID)
	return count
}

func (t *DeliveryTracker) GetUnacked(
	channelID uint16,
) []*TrackedDelivery {
	t.mu.RLock()
	defer t.mu.RUnlock()
	chMap := t.byChannel[channelID]
	out := make([]*TrackedDelivery, 0, len(chMap))
	for _, d := range chMap {
		out = append(out, d)
	}
	return out
}

// Count returns the total number of tracked deliveries.
func (t *DeliveryTracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	total := 0
	for _, chMap := range t.byChannel {
		total += len(chMap)
	}
	return total
}

// NackAll nacks every delivery for a channel, optionally requeueing.
func (t *DeliveryTracker) NackAll(
	channelID uint16,
	requeue bool,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	chMap := t.byChannel[channelID]
	if chMap == nil {
		return
	}
	if requeue {
		for _, d := range chMap {
			t.store.Enqueue(d.queueName, d.msg)
		}
	}
	delete(t.byChannel, channelID)
}
