// publisher.go coordinates end-to-end message publishing.
package server

import "fmt"

// Publisher routes published messages to queues and consumers.
type Publisher struct {
	exchanges  *ExchangeManager
	queues     *QueueManager
	bindings   *BindingManager
	store      *MessageStore
	consumers  *ConsumerManager
	deliverer  *Deliverer
	tracker    *DeliveryTracker
}

// NewPublisher creates a publisher with all required managers.
func NewPublisher(
	ex *ExchangeManager,
	qm *QueueManager,
	bm *BindingManager,
	store *MessageStore,
	cm *ConsumerManager,
	tracker *DeliveryTracker,
) *Publisher {
	d := NewDeliverer(cm, store, tracker)
	return &Publisher{
		exchanges: ex,
		queues:    qm,
		bindings:  bm,
		store:     store,
		consumers: cm,
		deliverer: d,
		tracker:   tracker,
	}
}

// Publish routes a message to bound queues and delivers to consumers.
func (p *Publisher) Publish(
	exchangeName, routingKey string,
	msg *Message,
	channelID uint16,
) error {
	ex, ok := p.exchanges.Get(exchangeName)
	if !ok {
		return fmt.Errorf("exchange %q not found", exchangeName)
	}

	bindings := p.bindings.GetBindings(exchangeName)
	queues := ex.Router().Route(routingKey, bindings)

	for _, qn := range queues {
		p.store.Enqueue(qn, msg)
		_ = p.deliverer.Deliver(msg, qn, channelID)
	}
	return nil
}
