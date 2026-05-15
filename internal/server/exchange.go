// exchange.go defines the Exchange abstraction and routing interface.
package server

// ExchangeType identifies the routing algorithm.
type ExchangeType string

const (
	ExchangeDirect  ExchangeType = "direct"
	ExchangeFanout  ExchangeType = "fanout"
	ExchangeTopic   ExchangeType = "topic"
	ExchangeHeaders ExchangeType = "headers"
)

// Exchange holds metadata for an AMQP exchange.
type Exchange struct {
	Name       string
	Type       ExchangeType
	Durable    bool
	AutoDelete bool
	Internal   bool
	Args       map[string]interface{}
}

// Router routes a message to queue names via bindings.
type Router interface {
	Route(routingKey string, headers map[string]interface{}, bindings []*Binding) []string
}

// NewExchange creates an exchange with the given properties.
func NewExchange(
	name string,
	exType ExchangeType,
	durable, autoDelete, internal bool,
	args map[string]interface{},
) *Exchange {
	if args == nil {
		args = map[string]interface{}{}
	}
	return &Exchange{
		Name:       name,
		Type:       exType,
		Durable:    durable,
		AutoDelete: autoDelete,
		Internal:   internal,
		Args:       args,
	}
}

// Router returns the appropriate router for this exchange type.
func (e *Exchange) Router() Router {
	switch e.Type {
	case ExchangeDirect:
		return &DirectExchange{}
	case ExchangeFanout:
		return &FanoutExchange{}
	case ExchangeTopic:
		return &TopicExchange{}
	case ExchangeHeaders:
		return &HeadersExchange{}
	}
	return nil
}
