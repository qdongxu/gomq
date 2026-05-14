// binding.go defines the Binding abstraction.
package server

// Binding links an exchange to a queue with a routing key.
type Binding struct {
	ExchangeName string
	QueueName    string
	RoutingKey   string
	Args         map[string]interface{}
}

// NewBinding creates a binding with the given properties.
func NewBinding(
	exchange, queue, routingKey string,
	args map[string]interface{},
) *Binding {
	if args == nil {
		args = map[string]interface{}{}
	}
	return &Binding{
		ExchangeName: exchange,
		QueueName:    queue,
		RoutingKey:   routingKey,
		Args:         args,
	}
}
