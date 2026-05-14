// tracked_delivery.go defines a single in-flight delivery record.
package server

import "time"

// TrackedDelivery holds metadata for a message awaiting client ack.
type TrackedDelivery struct {
	deliveryTag uint64
	msg         *Message
	queueName   string
	channelID   uint16
	sentAt      time.Time
}

// NewTrackedDelivery creates a record for the given message.
func NewTrackedDelivery(
	tag uint64,
	msg *Message,
	queueName string,
	channelID uint16,
) *TrackedDelivery {
	return &TrackedDelivery{
		deliveryTag: tag,
		msg:         msg,
		queueName:   queueName,
		channelID:   channelID,
		sentAt:      time.Now(),
	}
}

// DeliveryTag returns the unique delivery identifier.
func (d *TrackedDelivery) DeliveryTag() uint64 {
	return d.deliveryTag
}

// Message returns the underlying message.
func (d *TrackedDelivery) Message() *Message {
	return d.msg
}

// QueueName returns the source queue.
func (d *TrackedDelivery) QueueName() string {
	return d.queueName
}
