// e2e_binding.go defines exchange-to-exchange bindings.
package server

// E2EBinding links a source exchange to a destination exchange.
type E2EBinding struct {
	Source      string
	Destination string
	RoutingKey  string
	Args        map[string]interface{}
}

// NewE2EBinding creates an exchange-to-exchange binding.
func NewE2EBinding(
	source, destination, routingKey string,
	args map[string]interface{},
) *E2EBinding {
	if args == nil {
		args = map[string]interface{}{}
	}
	return &E2EBinding{
		Source:      source,
		Destination: destination,
		RoutingKey:  routingKey,
		Args:        args,
	}
}
