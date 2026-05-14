// deliverer.go delivers messages to consumers via channels.
package server

// Deliverer handles message delivery to subscribed consumers.
type Deliverer struct {
	consumers *ConsumerManager
	store     *MessageStore
	tracker   *DeliveryTracker
}

// NewDeliverer creates a deliverer with required managers.
func NewDeliverer(
	cm *ConsumerManager,
	store *MessageStore,
	tracker *DeliveryTracker,
) *Deliverer {
	return &Deliverer{
		consumers: cm,
		store:     store,
		tracker:   tracker,
	}
}

// Deliver sends a message to all consumers of the target queue.
func (d *Deliverer) Deliver(
	msg *Message,
	queueName string,
	channelID uint16,
) error {
	list := d.consumers.GetConsumers(queueName)
	for _, c := range list {
		tag := uint64(0) // delivery tag assigned by tracker
		msg.SetDeliveryTag(tag)
		if c.Channel() != nil {
			_ = c.Channel().SendFrame(nil) // placeholder frame
		}
		d.tracker.Record(tag, msg, queueName, channelID)
	}
	return nil
}
