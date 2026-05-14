// deliverer.go delivers messages to consumers via channels.
package server

// Deliverer handles message delivery to subscribed consumers.
type Deliverer struct {
	consumers *ConsumerManager
}

// NewDeliverer creates a deliverer with a consumer manager.
func NewDeliverer(cm *ConsumerManager) *Deliverer {
	return &Deliverer{consumers: cm}
}

// Deliver sends a message to all consumers of the target queue.
func (d *Deliverer) Deliver(msg *Message, queueName string) error {
	consumers := d.consumers.GetConsumers(queueName)
	for _, c := range consumers {
		_ = c.Channel().SendFrame(nil) // placeholder delivery
		_ = msg                        // use msg to avoid unused
	}
	return nil
}
