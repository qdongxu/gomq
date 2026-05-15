// exchange_fanout.go implements fanout (broadcast) routing.
package server

// FanoutExchange routes messages to all bound queues.
type FanoutExchange struct{}

// Route returns all unique queue names from bindings.
func (e *FanoutExchange) Route(
	_ string,
	_ map[string]interface{},
	bindings []*Binding,
) []string {
	var queues []string
	seen := make(map[string]struct{})
	for _, b := range bindings {
		if _, ok := seen[b.QueueName]; !ok {
			queues = append(queues, b.QueueName)
			seen[b.QueueName] = struct{}{}
		}
	}
	return queues
}
