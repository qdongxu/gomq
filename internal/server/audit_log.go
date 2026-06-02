// audit_log.go provides audit logging for management and security events.
package server

import (
	"fmt"
	"sync"
	"time"
)

// AuditEvent represents a single auditable action.
type AuditEvent struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"`     // info, warn, error
	Category  string    `json:"category"`  // connection, exchange, queue, binding, auth, acl, admin
	Action    string    `json:"action"`    // open, close, declare, delete, bind, unbind, authenticate, reject, etc.
	User      string    `json:"user"`      // username or "anonymous"
	Remote    string    `json:"remote"`    // remote address
	VHost     string    `json:"vhost"`     // virtual host
	Detail    string    `json:"detail"`    // human-readable detail
	Success   bool      `json:"success"`   // whether the action succeeded
}

// AuditLog collects auditable events with an in-memory ring buffer.
type AuditLog struct {
	enabled bool
	mu      sync.RWMutex
	events  []AuditEvent
	maxSize int
}

// NewAuditLog creates an audit log buffer. size <= 0 means unlimited.
func NewAuditLog(enabled bool, size int) *AuditLog {
	if size < 0 {
		size = 0
	}
	return &AuditLog{
		enabled: enabled,
		maxSize: size,
		events:  make([]AuditEvent, 0, size),
	}
}

// SetEnabled toggles audit logging at runtime.
func (a *AuditLog) SetEnabled(v bool) {
	a.mu.Lock()
	a.enabled = v
	a.mu.Unlock()
}

// Enabled returns whether audit logging is active.
func (a *AuditLog) Enabled() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.enabled
}

// Record adds an event to the log if enabled.
func (a *AuditLog) Record(e AuditEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.enabled {
		return
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}
	if a.maxSize > 0 && len(a.events) >= a.maxSize {
		// drop oldest
		copy(a.events, a.events[1:])
		a.events = a.events[:len(a.events)-1]
	}
	a.events = append(a.events, e)
}

// Recordf is a convenience wrapper around Record.
func (a *AuditLog) Recordf(level, category, action, user, remote, vhost, detail string, success bool) {
	a.Record(AuditEvent{
		Level:    level,
		Category: category,
		Action:   action,
		User:     user,
		Remote:   remote,
		VHost:    vhost,
		Detail:   detail,
		Success:  success,
	})
}

// Recent returns the last n events (or all if n <= 0).
func (a *AuditLog) Recent(n int) []AuditEvent {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if n <= 0 || n > len(a.events) {
		n = len(a.events)
	}
	if n == 0 {
		return nil
	}
	out := make([]AuditEvent, n)
	copy(out, a.events[len(a.events)-n:])
	return out
}

// All returns a copy of all events.
func (a *AuditLog) All() []AuditEvent {
	return a.Recent(0)
}

// Count returns the total number of events stored.
func (a *AuditLog) Count() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.events)
}

// SetMaxSize updates the ring buffer limit at runtime (size <= 0 unlimited).
func (a *AuditLog) SetMaxSize(n int) {
	a.mu.Lock()
	if n < 0 {
		n = 0
	}
	a.maxSize = n
	a.mu.Unlock()
}

// ConnectionOpened records a connection open event.
func (a *AuditLog) ConnectionOpened(remote, user, vhost string) {
	a.Recordf("info", "connection", "open", user, remote, vhost, fmt.Sprintf("connection opened from %s", remote), true)
}

// ConnectionClosed records a connection close event.
func (a *AuditLog) ConnectionClosed(remote, user, vhost string) {
	a.Recordf("info", "connection", "close", user, remote, vhost, fmt.Sprintf("connection closed from %s", remote), true)
}

// AuthFailure records an authentication failure.
func (a *AuditLog) AuthFailure(remote, user string, reason string) {
	a.Recordf("warn", "auth", "authenticate", user, remote, "/", fmt.Sprintf("auth failed: %s", reason), false)
}

// ACLDenied records an ACL denial.
func (a *AuditLog) ACLDenied(user, remote, vhost, resource, permission string) {
	a.Recordf("warn", "acl", "reject", user, remote, vhost, fmt.Sprintf("ACL denied %s on %s", permission, resource), false)
}

// ExchangeDeclared records an exchange declaration.
func (a *AuditLog) ExchangeDeclared(user, remote, vhost, name, exType string, durable bool) {
	a.Recordf("info", "exchange", "declare", user, remote, vhost, fmt.Sprintf("exchange %s (%s, durable=%v)", name, exType, durable), true)
}

// ExchangeDeleted records an exchange deletion.
func (a *AuditLog) ExchangeDeleted(user, remote, vhost, name string) {
	a.Recordf("info", "exchange", "delete", user, remote, vhost, fmt.Sprintf("exchange %s deleted", name), true)
}

// QueueDeclared records a queue declaration.
func (a *AuditLog) QueueDeclared(user, remote, vhost, name string, durable bool) {
	a.Recordf("info", "queue", "declare", user, remote, vhost, fmt.Sprintf("queue %s (durable=%v)", name, durable), true)
}

// QueueDeleted records a queue deletion.
func (a *AuditLog) QueueDeleted(user, remote, vhost, name string) {
	a.Recordf("info", "queue", "delete", user, remote, vhost, fmt.Sprintf("queue %s deleted", name), true)
}

// BindingCreated records a binding creation.
func (a *AuditLog) BindingCreated(user, remote, vhost, exchange, queue, key string) {
	a.Recordf("info", "binding", "bind", user, remote, vhost, fmt.Sprintf("%s -> %s (key=%s)", exchange, queue, key), true)
}

// BindingRemoved records a binding removal.
func (a *AuditLog) BindingRemoved(user, remote, vhost, exchange, queue, key string) {
	a.Recordf("info", "binding", "unbind", user, remote, vhost, fmt.Sprintf("%s -> %s (key=%s)", exchange, queue, key), true)
}
