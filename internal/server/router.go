// router.go provides the unified message routing entry point.
package server

import (
	"fmt"
)

// MessageRouter ties together exchange, queue, and binding managers
// to route messages from publishers to queues.
type MessageRouter struct {
	exchanges *ExchangeManager
	queues    *QueueManager
	bindings  *BindingManager
}

// NewMessageRouter creates a router with the given managers.
func NewMessageRouter(
	ex *ExchangeManager,
	q *QueueManager,
	b *BindingManager,
) *MessageRouter {
	return &MessageRouter{
		exchanges: ex,
		queues:    q,
		bindings:  b,
	}
}

// Route resolves an exchange and routing key to a list of queue names.
func (r *MessageRouter) Route(
	exchangeName, routingKey string,
	headers map[string]interface{},
) ([]string, error) {
	ex, ok := r.exchanges.Get(exchangeName)
	if !ok {
		return nil, fmt.Errorf("exchange %q not found", exchangeName)
	}

	router := ex.Router()
	if router == nil {
		return nil, fmt.Errorf(
			"no router for exchange type %q", ex.Type,
		)
	}

	bindings := r.bindings.GetBindings(exchangeName)
	queues := router.Route(routingKey, headers, bindings)
	return queues, nil
}

// DeleteExchange removes an exchange and cascades to bindings.
func (r *MessageRouter) DeleteExchange(name string) error {
	if err := r.exchanges.Delete(name); err != nil {
		return err
	}
	r.bindings.UnbindAllForExchange(name)
	return nil
}

// DeleteQueue removes a queue and cascades to bindings.
func (r *MessageRouter) DeleteQueue(name string) error {
	if err := r.queues.Delete(name); err != nil {
		return err
	}
	r.bindings.UnbindAllForQueue(name)
	return nil
}
