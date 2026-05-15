// ttl.go implements message and queue TTL support.
package server

import (
	"strconv"
	"time"
)

// MessageTTL returns the per-message TTL from the Expiration property.
// AMQP Expiration is expressed in milliseconds as a string.
func MessageTTL(msg *Message) time.Duration {
	exp := msg.properties.Expiration
	if exp == "" {
		return 0
	}
	ms, err := strconv.ParseInt(exp, 10, 64)
	if err != nil {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// QueueTTL returns the per-queue TTL from queue arguments.
func QueueTTL(args map[string]interface{}) time.Duration {
	if args == nil {
		return 0
	}
	v, ok := args["x-message-ttl"]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int:
		return time.Duration(n) * time.Millisecond
	case int8:
		return time.Duration(n) * time.Millisecond
	case int16:
		return time.Duration(n) * time.Millisecond
	case int32:
		return time.Duration(n) * time.Millisecond
	case int64:
		return time.Duration(n) * time.Millisecond
	case uint:
		return time.Duration(n) * time.Millisecond
	case uint8:
		return time.Duration(n) * time.Millisecond
	case uint16:
		return time.Duration(n) * time.Millisecond
	case uint32:
		return time.Duration(n) * time.Millisecond
	case uint64:
		return time.Duration(n) * time.Millisecond
	case float32:
		return time.Duration(n) * time.Millisecond
	case float64:
		return time.Duration(n) * time.Millisecond
	}
	return 0
}

// IsExpired reports whether a message has exceeded its TTL.
// It checks message-level TTL first, falling back to queue-level.
func IsExpired(msg *Message, queueArgs map[string]interface{}) bool {
	ttl := MessageTTL(msg)
	if ttl == 0 {
		ttl = QueueTTL(queueArgs)
	}
	if ttl == 0 || msg.EnqueuedAt().IsZero() {
		return false
	}
	return time.Since(msg.EnqueuedAt()) >= ttl
}
