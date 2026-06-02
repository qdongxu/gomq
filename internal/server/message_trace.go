// message_trace.go provides message lifecycle tracing for observability.
package server

import (
	"fmt"
	"sync"
	"time"
)

// TraceStage represents a point in a message's lifecycle.
type TraceStage string

const (
	TracePublished  TraceStage = "published"
	TraceRouted     TraceStage = "routed"
	TraceDelivered  TraceStage = "delivered"
	TraceAcked      TraceStage = "acked"
	TraceNacked     TraceStage = "nacked"
	TraceRejected   TraceStage = "rejected"
	TraceDeadLetter TraceStage = "dead_letter"
)

// MessageTrace records a single stage in a message's lifecycle.
type MessageTrace struct {
	MessageID  string    `json:"message_id"`   // delivery tag or generated UUID
	Exchange   string    `json:"exchange"`
	RoutingKey string    `json:"routing_key"`
	Queue      string    `json:"queue,omitempty"` // set on deliver / ack stages
	Stage      string    `json:"stage"`
	NodeID     string    `json:"node_id,omitempty"` // cluster node ID
	Timestamp  time.Time `json:"timestamp"`
	Detail     string    `json:"detail,omitempty"`
}

// MessageTracer is a ring-buffered message lifecycle tracer.
type MessageTracer struct {
	enabled bool
	mu      sync.RWMutex
	traces  []MessageTrace
	maxSize int
}

// NewMessageTracer creates a tracer. size <= 0 means unlimited.
func NewMessageTracer(enabled bool, size int) *MessageTracer {
	if size < 0 {
		size = 0
	}
	return &MessageTracer{
		enabled: enabled,
		maxSize: size,
		traces:  make([]MessageTrace, 0, size),
	}
}

// SetEnabled toggles tracing at runtime.
func (t *MessageTracer) SetEnabled(v bool) {
	t.mu.Lock()
	t.enabled = v
	t.mu.Unlock()
}

// Enabled returns whether tracing is active.
func (t *MessageTracer) Enabled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.enabled
}

// Record adds a trace entry if enabled.
func (t *MessageTracer) Record(tr MessageTrace) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.enabled {
		return
	}
	if tr.Timestamp.IsZero() {
		tr.Timestamp = time.Now()
	}
	if t.maxSize > 0 && len(t.traces) >= t.maxSize {
		copy(t.traces, t.traces[1:])
		t.traces = t.traces[:len(t.traces)-1]
	}
	t.traces = append(t.traces, tr)
}

// Recordf is a convenience wrapper.
func (t *MessageTracer) Recordf(msgID, exchange, routingKey, queue, stage, nodeID, detail string) {
	t.Record(MessageTrace{
		MessageID:  msgID,
		Exchange:   exchange,
		RoutingKey: routingKey,
		Queue:      queue,
		Stage:      stage,
		NodeID:     nodeID,
		Detail:     detail,
	})
}

// Recent returns the last n traces (or all if n <= 0).
func (t *MessageTracer) Recent(n int) []MessageTrace {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if n <= 0 || n > len(t.traces) {
		n = len(t.traces)
	}
	if n == 0 {
		return nil
	}
	out := make([]MessageTrace, n)
	copy(out, t.traces[len(t.traces)-n:])
	return out
}

// All returns a copy of all traces.
func (t *MessageTracer) All() []MessageTrace {
	return t.Recent(0)
}

// Count returns the total number of traces stored.
func (t *MessageTracer) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.traces)
}

// SetMaxSize updates the ring buffer limit at runtime (size <= 0 unlimited).
func (t *MessageTracer) SetMaxSize(n int) {
	t.mu.Lock()
	if n < 0 {
		n = 0
	}
	t.maxSize = n
	t.mu.Unlock()
}

// TracePublished records a publish event.
func (t *MessageTracer) TracePublished(msgID, exchange, routingKey, nodeID string) {
	t.Recordf(msgID, exchange, routingKey, "", string(TracePublished), nodeID, fmt.Sprintf("published to %s", exchange))
}

// TraceRouted records a routing event.
func (t *MessageTracer) TraceRouted(msgID, exchange, routingKey, queue, nodeID string) {
	t.Recordf(msgID, exchange, routingKey, queue, string(TraceRouted), nodeID, fmt.Sprintf("routed to queue %s", queue))
}

// TraceDelivered records a delivery event.
func (t *MessageTracer) TraceDelivered(msgID, queue, nodeID string) {
	t.Recordf(msgID, "", "", queue, string(TraceDelivered), nodeID, fmt.Sprintf("delivered from %s", queue))
}

// TraceAcked records an ack event.
func (t *MessageTracer) TraceAcked(msgID, queue, nodeID string) {
	t.Recordf(msgID, "", "", queue, string(TraceAcked), nodeID, "acknowledged")
}

// TraceNacked records a nack event.
func (t *MessageTracer) TraceNacked(msgID, queue, nodeID string, requeue bool) {
	t.Recordf(msgID, "", "", queue, string(TraceNacked), nodeID, fmt.Sprintf("nacked (requeue=%v)", requeue))
}

// TraceRejected records a reject event.
func (t *MessageTracer) TraceRejected(msgID, queue, nodeID string, requeue bool) {
	t.Recordf(msgID, "", "", queue, string(TraceRejected), nodeID, fmt.Sprintf("rejected (requeue=%v)", requeue))
}

// TraceDeadLetter records a dead-letter event.
func (t *MessageTracer) TraceDeadLetter(msgID, queue, dlx, nodeID string) {
	t.Recordf(msgID, "", "", queue, string(TraceDeadLetter), nodeID, fmt.Sprintf("dead-lettered to %s", dlx))
}
