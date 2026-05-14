// message.go defines the Message abstraction and its properties.
package server

import "time"

// Properties holds AMQP message metadata.
type Properties struct {
	ContentType     string
	ContentEncoding string
	Headers         map[string]interface{}
	DeliveryMode    uint8 // 1=non-persistent, 2=persistent
	Priority        uint8
	CorrelationID   string
	ReplyTo         string
	Expiration      string
	MessageID       string
	Timestamp       time.Time
	Type            string
	UserID          string
	AppID           string
	ClusterID       string
}

// Message represents an AMQP message.
type Message struct {
	payload     []byte
	properties  Properties
	exchange    string
	routingKey  string
	deliveryTag uint64
}

// NewMessage creates a message with payload and properties.
func NewMessage(payload []byte, props Properties) *Message {
	return &Message{
		payload:    payload,
		properties: props,
	}
}

// Payload returns the message body.
func (m *Message) Payload() []byte {
	return m.payload
}

// Properties returns the message metadata.
func (m *Message) Properties() Properties {
	return m.properties
}

// DeliveryTag returns the unique delivery identifier.
func (m *Message) DeliveryTag() uint64 {
	return m.deliveryTag
}

// SetDeliveryTag assigns a delivery tag.
func (m *Message) SetDeliveryTag(tag uint64) {
	m.deliveryTag = tag
}

// Exchange returns the source exchange name.
func (m *Message) Exchange() string {
	return m.exchange
}

// RoutingKey returns the routing key used at publish time.
func (m *Message) RoutingKey() string {
	return m.routingKey
}

// SetRoutingMeta sets exchange and routing key for routing.
func (m *Message) SetRoutingMeta(ex, key string) {
	m.exchange = ex
	m.routingKey = key
}
