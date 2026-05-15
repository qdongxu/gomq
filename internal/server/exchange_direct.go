// exchange_direct.go implements direct (exact-match) routing.
package server

// DirectExchange routes messages by exact routing key match.
type DirectExchange struct{}

// Route returns queue names whose binding routing key equals msgRoutingKey.
func (e *DirectExchange) Route(
	msgRoutingKey string,
	_ map[string]interface{},
	bindings []*Binding,
) []string {
	var queues []string
	seen := make(map[string]struct{})
	for _, b := range bindings {
		if b.RoutingKey == msgRoutingKey {
			if _, ok := seen[b.QueueName]; !ok {
				queues = append(queues, b.QueueName)
				seen[b.QueueName] = struct{}{}
			}
		}
	}
	return queues
}
