// pull_consumer.go implements the basic.get pull model.
package server

import "sync/atomic"

// PullConsumer fetches messages on demand from a queue.
type PullConsumer struct {
	store   *MessageStore
	tracker *DeliveryTracker
	tagSeq  uint64
}

// NewPullConsumer creates a pull consumer.
func NewPullConsumer(
	store *MessageStore,
	tracker *DeliveryTracker,
) *PullConsumer {
	return &PullConsumer{
		store:   store,
		tracker: tracker,
	}
}

// Get removes and returns one message from the named queue.
// When autoAck is false the delivery is tracked.
func (pc *PullConsumer) Get(
	queueName string,
	autoAck bool,
	channelID uint16,
) (*Message, bool) {
	msg, ok := pc.store.Dequeue(queueName)
	if !ok {
		return nil, false
	}
	if !autoAck {
		tag := atomic.AddUint64(&pc.tagSeq, 1)
		msg.SetDeliveryTag(tag)
		pc.tracker.Record(tag, msg, queueName, channelID)
	}
	return msg, true
}
