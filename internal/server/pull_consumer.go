// pull_consumer.go implements the basic.get pull model.
package server

import (
	"sync/atomic"
)

// PullConsumer fetches messages on demand from a queue.
type PullConsumer struct {
	store     *MessageStore
	tracker   *DeliveryTracker
	queueMgr  *QueueManager
	tagSeq    uint64
}

// NewPullConsumer creates a pull consumer.
func NewPullConsumer(
	store *MessageStore,
	tracker *DeliveryTracker,
) *PullConsumer {
	return NewPullConsumerWithQueueManager(store, tracker, nil)
}

// NewPullConsumerWithQueueManager creates a pull consumer that can
// resolve queue arguments for TTL checks.
func NewPullConsumerWithQueueManager(
	store *MessageStore,
	tracker *DeliveryTracker,
	qm *QueueManager,
) *PullConsumer {
	return &PullConsumer{
		store:    store,
		tracker:  tracker,
		queueMgr: qm,
	}
}

// Get removes and returns one message from the named queue.
// When autoAck is false the delivery is tracked.
// Expired messages are silently skipped.
func (pc *PullConsumer) Get(
	queueName string,
	autoAck bool,
	channelID uint16,
) (*Message, bool) {
	var queueArgs map[string]interface{}
	if pc.queueMgr != nil {
		if q, ok := pc.queueMgr.Get(queueName); ok {
			queueArgs = q.Args
		}
	}

	for {
		msg, ok := pc.store.Dequeue(queueName)
		if !ok {
			return nil, false
		}
		if IsExpired(msg, queueArgs) {
			continue
		}
		if !autoAck {
			tag := atomic.AddUint64(&pc.tagSeq, 1)
			msg.SetDeliveryTag(tag)
			pc.tracker.Record(tag, msg, queueName, channelID)
		}
		return msg, true
	}
}
