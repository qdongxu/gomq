// consumer_manager.go manages consumer subscriptions per queue.
package server

import (
	"fmt"
	"sync"

	"github.com/qdongxu/gomq/internal/metrics"
)

// ConsumerManager tracks all active consumers.
type ConsumerManager struct {
	byQueue   map[string][]*Consumer
	byTag     map[string]*Consumer
	mu        sync.RWMutex
	metrics   metrics.Collector
	groupMgr  *ConsumerGroupManager
}

// SetGroupManager injects the consumer group manager.
func (m *ConsumerManager) SetGroupManager(gm *ConsumerGroupManager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groupMgr = gm
}

// GroupManager returns the consumer group manager.
func (m *ConsumerManager) GroupManager() *ConsumerGroupManager {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.groupMgr
}

// NewConsumerManager creates an empty consumer manager.
func NewConsumerManager() *ConsumerManager {
	return &ConsumerManager{
		byQueue: make(map[string][]*Consumer),
		byTag:   make(map[string]*Consumer),
		metrics: &metrics.NoOp{},
	}
}

// SetMetrics configures the metrics collector.
func (m *ConsumerManager) SetMetrics(mc metrics.Collector) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics = mc
}

// Subscribe registers a consumer for a queue. If the consumer
// specifies x-group-id in args, it joins the corresponding group
// for load-balanced consumption.
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

	groupID := ""
	strategy := "round-robin"
	if args != nil {
		if g, ok := args["x-group-id"].(string); ok && g != "" {
			groupID = g
		}
		if s, ok := args["x-group-strategy"].(string); ok && s != "" {
			strategy = s
		}
	}

	c := NewConsumer(tag, queueName, ch, autoAck, false, exclusive, args, groupID)
	m.byQueue[queueName] = append(m.byQueue[queueName], c)
	m.byTag[tag] = c

	if groupID != "" && m.groupMgr != nil {
		m.groupMgr.Join(groupID, queueName, c, strategy)
	}

	m.metrics.ConsumerAdded()
	return c, nil
}

// Unsubscribe removes a consumer by tag. If the consumer belongs
// to a group, it is also removed from the group.
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

	if c.groupID != "" && m.groupMgr != nil {
		m.groupMgr.Leave(tag)
	}

	m.metrics.ConsumerRemoved()
	return nil
}

// CancelByChannel unsubscribes all consumers on the given channel.
// If x-cancel-on-ha-failover is set, those consumers are removed;
// otherwise they are left in place.
func (m *ConsumerManager) CancelByChannel(ch *Channel) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	cancelled := 0
	for tag, c := range m.byTag {
		if c.channel != ch {
			continue
		}

		// Check x-cancel-on-ha-failover flag.
		if cancel, ok := c.args["x-cancel-on-ha-failover"].(bool); ok && !cancel {
			continue
		}

		delete(m.byTag, tag)

		list := m.byQueue[c.queueName]
		for i, existing := range list {
			if existing.tag == tag {
				m.byQueue[c.queueName] = append(list[:i], list[i+1:]...)
				break
			}
		}

		if c.groupID != "" && m.groupMgr != nil {
			m.groupMgr.Leave(tag)
		}

		cancelled++
		m.metrics.ConsumerRemoved()
	}
	return cancelled
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
