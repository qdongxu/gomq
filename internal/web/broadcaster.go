// broadcaster.go broadcasts server events to WebSocket clients.
package web

import (
	"encoding/json"
	"time"
)

// EventType defines the kind of server event.
type EventType string

const (
	EventConnectionOpen      EventType = "connection_open"
	EventConnectionClose     EventType = "connection_close"
	EventMessagePublished    EventType = "message_published"
	EventQueueDepthChanged   EventType = "queue_depth_changed"
	EventConsumerAdded       EventType = "consumer_added"
	EventConsumerRemoved     EventType = "consumer_removed"
)

// Event is a server event sent to WebSocket clients.
type Event struct {
	Type      EventType       `json:"type"`
	Timestamp string          `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// ConnectionEventPayload holds connection event data.
type ConnectionEventPayload struct {
	RemoteAddr string `json:"remote_addr"`
	State      string `json:"state"`
}

// MessageEventPayload holds message published event data.
type MessageEventPayload struct {
	Queue      string `json:"queue"`
	DeliveryTag uint64 `json:"delivery_tag"`
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
}

// QueueDepthEventPayload holds queue depth change event data.
type QueueDepthEventPayload struct {
	Queue    string `json:"queue"`
	Depth    int    `json:"depth"`
	Previous int    `json:"previous"`
}

// ConsumerEventPayload holds consumer event data.
type ConsumerEventPayload struct {
	Queue     string `json:"queue"`
	ChannelID uint16 `json:"channel_id"`
	Tag       string `json:"tag"`
}

// Broadcaster wraps a Hub and provides typed event helpers.
type Broadcaster struct {
	hub *Hub
}

// NewBroadcaster creates a broadcaster.
func NewBroadcaster(hub *Hub) *Broadcaster {
	return &Broadcaster{hub: hub}
}

// BroadcastConnectionOpen sends a connection_open event.
func (b *Broadcaster) BroadcastConnectionOpen(remoteAddr, state string) {
	payload, _ := json.Marshal(ConnectionEventPayload{
		RemoteAddr: remoteAddr,
		State:      state,
	})
	b.broadcast(EventConnectionOpen, payload)
}

// BroadcastConnectionClose sends a connection_close event.
func (b *Broadcaster) BroadcastConnectionClose(remoteAddr, state string) {
	payload, _ := json.Marshal(ConnectionEventPayload{
		RemoteAddr: remoteAddr,
		State:      state,
	})
	b.broadcast(EventConnectionClose, payload)
}

// BroadcastMessagePublished sends a message_published event.
func (b *Broadcaster) BroadcastMessagePublished(queue string, deliveryTag uint64, exchange, routingKey string) {
	payload, _ := json.Marshal(MessageEventPayload{
		Queue:       queue,
		DeliveryTag: deliveryTag,
		Exchange:    exchange,
		RoutingKey:  routingKey,
	})
	b.broadcast(EventMessagePublished, payload)
}

// BroadcastQueueDepthChanged sends a queue_depth_changed event.
func (b *Broadcaster) BroadcastQueueDepthChanged(queue string, depth, previous int) {
	payload, _ := json.Marshal(QueueDepthEventPayload{
		Queue:    queue,
		Depth:    depth,
		Previous: previous,
	})
	b.broadcast(EventQueueDepthChanged, payload)
}

// BroadcastConsumerAdded sends a consumer_added event.
func (b *Broadcaster) BroadcastConsumerAdded(queue string, channelID uint16, tag string) {
	payload, _ := json.Marshal(ConsumerEventPayload{
		Queue:     queue,
		ChannelID: channelID,
		Tag:       tag,
	})
	b.broadcast(EventConsumerAdded, payload)
}

// BroadcastConsumerRemoved sends a consumer_removed event.
func (b *Broadcaster) BroadcastConsumerRemoved(queue string, channelID uint16, tag string) {
	payload, _ := json.Marshal(ConsumerEventPayload{
		Queue:     queue,
		ChannelID: channelID,
		Tag:       tag,
	})
	b.broadcast(EventConsumerRemoved, payload)
}

func (b *Broadcaster) broadcast(t EventType, payload json.RawMessage) {
	if b.hub == nil {
		return
	}
	b.hub.Broadcast(Event{
		Type:      t,
		Timestamp: time.Now().Format(time.RFC3339),
		Payload:   payload,
	})
}
