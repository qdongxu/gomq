// queue.go defines the Queue abstraction and its properties.
package server

import (
	"fmt"
	"sync"
)

// QueueState represents the operational state of a queue.
type QueueState int

const (
	QueueActive QueueState = iota
	QueueFlow
	QueueEmpty
)

// Queue holds metadata for an AMQP message queue.
type Queue struct {
	Name       string
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	Args       map[string]interface{}
	state      QueueState
	mu         sync.RWMutex
	owner      *Connection // non-nil for exclusive queues
}

// NewQueue creates a queue with the given properties.
func NewQueue(
	name string,
	durable, exclusive, autoDelete bool,
	args map[string]interface{},
	owner *Connection,
) *Queue {
	if args == nil {
		args = map[string]interface{}{}
	}
	return &Queue{
		Name:       name,
		Durable:    durable,
		Exclusive:  exclusive,
		AutoDelete: autoDelete,
		Args:       args,
		state:      QueueActive,
		owner:      owner,
	}
}

// State returns the current queue state safely.
func (q *Queue) State() QueueState {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.state
}

// SetState updates the queue state safely.
func (q *Queue) SetState(s QueueState) {
	q.mu.Lock()
	q.state = s
	q.mu.Unlock()
}

// IsExclusive reports whether this queue is exclusive.
func (q *Queue) IsExclusive() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.Exclusive
}

// Owner returns the connection that owns an exclusive queue.
func (q *Queue) Owner() *Connection {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.owner
}

// MatchArgs checks whether the given arguments match the queue's.
func (q *Queue) MatchArgs(
	durable, exclusive, autoDelete bool,
	args map[string]interface{},
) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.Durable != durable || q.Exclusive != exclusive ||
		q.AutoDelete != autoDelete {
		return false
	}
	return argsEqual(q.Args, args)
}

// argsEqual compares two argument maps shallowly.
func argsEqual(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || fmt.Sprintf("%v", av) != fmt.Sprintf("%v", bv) {
			return false
		}
	}
	return true
}
