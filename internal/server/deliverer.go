// deliverer.go is a placeholder for message delivery to consumers.
// Full consumer management will be implemented in Issue #18.
package server

// Deliverer handles sending messages to a consumer channel.
type Deliverer struct{}

// NewDeliverer creates a deliverer.
func NewDeliverer() *Deliverer {
	return &Deliverer{}
}

// Deliver sends a message to the given channel.
func (d *Deliverer) Deliver(msg *Message, ch *Channel) error {
	return ch.SendFrame(nil) // placeholder
}
