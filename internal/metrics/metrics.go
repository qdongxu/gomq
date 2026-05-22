// metrics.go defines the metrics collection interface used by the
// server package. A no-op implementation is provided so that server
// code can safely call metrics methods even when metrics are
// disabled.
package metrics

// Collector is the interface for recording broker metrics.
type Collector interface {
	ConnectionOpened()
	ConnectionClosed()
	MessagePublished()
	MessageConsumed()
	MessageAcked()
	MessageNacked()
	QueueDeclared()
	QueueDeleted()
	ConsumerAdded()
	ConsumerRemoved()
	NodeUp()
	NodeDown()
}

// NoOp is a no-op Collector.
type NoOp struct{}

var _ Collector = (*NoOp)(nil)

func (n *NoOp) ConnectionOpened()  {}
func (n *NoOp) ConnectionClosed()  {}
func (n *NoOp) MessagePublished()  {}
func (n *NoOp) MessageConsumed()   {}
func (n *NoOp) MessageAcked()      {}
func (n *NoOp) MessageNacked()     {}
func (n *NoOp) QueueDeclared()     {}
func (n *NoOp) QueueDeleted()      {}
func (n *NoOp) ConsumerAdded()     {}
func (n *NoOp) ConsumerRemoved()   {}
func (n *NoOp) NodeUp()            {}
func (n *NoOp) NodeDown()          {}
