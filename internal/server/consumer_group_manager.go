// consumer_group_manager.go manages all consumer groups.
package server

import (
	"sync"
)

// ConsumerGroupManager tracks consumer groups per queue.
type ConsumerGroupManager struct {
	byID   map[string]*ConsumerGroup
	byQueue map[string][]*ConsumerGroup
	mu     sync.RWMutex
}

// NewConsumerGroupManager creates an empty manager.
func NewConsumerGroupManager() *ConsumerGroupManager {
	return &ConsumerGroupManager{
		byID:    make(map[string]*ConsumerGroup),
		byQueue: make(map[string][]*ConsumerGroup),
	}
}

// Create registers a new consumer group. Returns nil if the
// group already exists.
func (m *ConsumerGroupManager) Create(
	id, queue, strategy string,
) *ConsumerGroup {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.byID[id]; ok {
		return nil
	}

	g := NewConsumerGroup(id, queue, strategy)
	m.byID[id] = g
	m.byQueue[queue] = append(m.byQueue[queue], g)
	return g
}

// Get looks up a group by ID.
func (m *ConsumerGroupManager) Get(id string) (*ConsumerGroup, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	g, ok := m.byID[id]
	return g, ok
}

// GetByQueue returns all groups for a queue.
func (m *ConsumerGroupManager) GetByQueue(queue string) []*ConsumerGroup {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*ConsumerGroup, len(m.byQueue[queue]))
	copy(out, m.byQueue[queue])
	return out
}

// List returns all groups.
func (m *ConsumerGroupManager) List() []*ConsumerGroup {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*ConsumerGroup, 0, len(m.byID))
	for _, g := range m.byID {
		out = append(out, g)
	}
	return out
}

// Delete removes a group by ID and all its members.
func (m *ConsumerGroupManager) Delete(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	g, ok := m.byID[id]
	if !ok {
		return false
	}
	delete(m.byID, id)

	list := m.byQueue[g.Queue()]
	for i, existing := range list {
		if existing.ID() == id {
			m.byQueue[g.Queue()] = append(list[:i], list[i+1:]...)
			break
		}
	}
	return true
}

// Join adds a consumer to a group, creating the group if needed.
func (m *ConsumerGroupManager) Join(
	id, queue string,
	c *Consumer,
	strategy string,
) *ConsumerGroup {
	m.mu.Lock()
	defer m.mu.Unlock()

	g, ok := m.byID[id]
	if !ok {
		g = NewConsumerGroup(id, queue, strategy)
		m.byID[id] = g
		m.byQueue[queue] = append(m.byQueue[queue], g)
	}
	g.Add(c)
	return g
}

// Leave removes a consumer from its group. If the group becomes
// empty, the group is deleted.
func (m *ConsumerGroupManager) Leave(tag string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, g := range m.byID {
		if g.Remove(tag) {
			if g.Count() == 0 {
				delete(m.byID, id)
				list := m.byQueue[g.Queue()]
				for i, existing := range list {
					if existing.ID() == id {
						m.byQueue[g.Queue()] = append(list[:i], list[i+1:]...)
						break
					}
				}
			}
			return true
		}
	}
	return false
}

// Select returns the consumer in the group that should receive the
// message with the given routing key. Returns nil if no group or
// no members.
func (m *ConsumerGroupManager) Select(
	groupID, routingKey string,
) *Consumer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	g, ok := m.byID[groupID]
	if !ok {
		return nil
	}
	return g.Select(routingKey)
}
