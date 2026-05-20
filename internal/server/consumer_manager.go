// consumer_manager.go manages consumer subscriptions per queue.
package server

import (
	"fmt"
	"sync"
)

// ConsumerManager tracks all active consumers.
type ConsumerManager struct {
	byQueue map[string][]*Consumer
	byTag   map[string]*Consumer
	mu      sync.RWMutex
}

// NewConsumerManager creates an empty consumer manager.
func NewConsumerManager() *ConsumerManager {
	return &ConsumerManager{
		byQueue: make(map[string][]*Consumer),
		byTag:   make(map[string]*Consumer),
	}
}

// Subscribe registers a consumer for a queue.
func (m *ConsumerManager) Subscribe(
	tag, queueName string,
	ch *Channel,
	autoAck, exclusive bool,
	args map[string]interface{},
) (*Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.byTag[tag]; ok {
		return nil, fmt.Errorf("consumer tag %q already exists", tag)
	}

	if exclusive {
		for _, c := range m.byQueue[queueName] {
			if c.Exclusive() {
				return nil, fmt.Errorf(
					"queue %q already has exclusive consumer", queueName,
				)
			}
		}
	}

	c := NewConsumer(tag, queueName, ch, autoAck, false, exclusive, args)
	m.byQueue[queueName] = append(m.byQueue[queueName], c)
	m.byTag[tag] = c
	return c, nil
}

// Unsubscribe removes a consumer by tag.
func (m *ConsumerManager) Unsubscribe(tag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.byTag[tag]
	if !ok {
		return fmt.Errorf("consumer %q not found", tag)
	}
	delete(m.byTag, tag)

	list := m.byQueue[c.queueName]
	for i, existing := range list {
		if existing.tag == tag {
			m.byQueue[c.queueName] = append(list[:i], list[i+1:]...)
			break
		}
	}
	return nil
}

// GetConsumers returns all consumers for a queue.
func (m *ConsumerManager) GetConsumers(queueName string) []*Consumer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Consumer, len(m.byQueue[queueName]))
	copy(out, m.byQueue[queueName])
	return out
}

// GetConsumer looks up a consumer by tag.
func (m *ConsumerManager) GetConsumer(tag string) (*Consumer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.byTag[tag]
	return c, ok
}

// CountByChannel returns the number of consumers on the given channel.
func (m *ConsumerManager) CountByChannel(ch *Channel) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, c := range m.byTag {
		if c.channel == ch {
			count++
		}
	}
	return count
}

// Count returns the total number of consumers.
func (m *ConsumerManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.byTag)
}
