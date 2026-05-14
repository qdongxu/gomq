// consumer.go defines the Consumer abstraction.
package server

// Consumer represents a client subscription to a queue.
type Consumer struct {
	tag        string
	queueName  string
	channel    *Channel
	autoAck    bool
	noLocal    bool
	exclusive  bool
	args       map[string]interface{}
}

// NewConsumer creates a consumer with the given properties.
func NewConsumer(
	tag, queueName string,
	ch *Channel,
	autoAck, noLocal, exclusive bool,
	args map[string]interface{},
) *Consumer {
	if args == nil {
		args = map[string]interface{}{}
	}
	return &Consumer{
		tag:       tag,
		queueName: queueName,
		channel:   ch,
		autoAck:   autoAck,
		noLocal:   noLocal,
		exclusive: exclusive,
		args:      args,
	}
}

// Tag returns the consumer identifier.
func (c *Consumer) Tag() string {
	return c.tag
}

// QueueName returns the subscribed queue.
func (c *Consumer) QueueName() string {
	return c.queueName
}

// Channel returns the consumer's channel.
func (c *Consumer) Channel() *Channel {
	return c.channel
}

// AutoAck reports whether messages are auto-acknowledged.
func (c *Consumer) AutoAck() bool {
	return c.autoAck
}

// Exclusive reports whether this consumer is exclusive.
func (c *Consumer) Exclusive() bool {
	return c.exclusive
}
