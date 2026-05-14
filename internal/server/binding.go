// binding.go is a placeholder for the full binding implementation.
// It provides the minimal Binding struct needed by Exchange routing.
package server

// Binding links an exchange to a queue with a routing key.
type Binding struct {
	ExchangeName string
	QueueName    string
	RoutingKey   string
	Args         map[string]interface{}
}
