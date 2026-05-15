// message_store.go provides in-memory per-queue message storage.
package server

import (
	"sync"
)

// MessageStore holds messages indexed by queue name.
type MessageStore struct {
	queues map[string][]*Message
	mu     sync.RWMutex
}

// NewMessageStore creates an empty message store.
func NewMessageStore() *MessageStore {
	return &MessageStore{
		queues: make(map[string][]*Message),
	}
}

// Enqueue appends a message to the named queue.
func (s *MessageStore) Enqueue(queueName string, msg *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues[queueName] = append(s.queues[queueName], msg)
}

// Dequeue removes and returns the first message of the named queue.
func (s *MessageStore) Dequeue(queueName string) (*Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[queueName]
	if len(q) == 0 {
		return nil, false
	}
	msg := q[0]
	s.queues[queueName] = q[1:]
	return msg, true
}

// Peek returns the first message without removing it.
func (s *MessageStore) Peek(queueName string) (*Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := s.queues[queueName]
	if len(q) == 0 {
		return nil, false
	}
	return q[0], true
}

// Len returns the number of messages in the named queue.
func (s *MessageStore) Len(queueName string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.queues[queueName])
}

// Purge clears all messages from the named queue.
func (s *MessageStore) Purge(queueName string) {
	s.mu.Lock()
	delete(s.queues, queueName)
	s.mu.Unlock()
}

// EnqueuePriority inserts a message maintaining descending priority order.
func (s *MessageStore) EnqueuePriority(queueName string, msg *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues[queueName] = insertByPriority(
		s.queues[queueName], msg,
	)
}
func (s *MessageStore) AllQueues() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.queues))
	for name := range s.queues {
		out = append(out, name)
	}
	return out
}