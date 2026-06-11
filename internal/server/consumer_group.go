// consumer_group.go implements consumer groups for load-balanced
// message consumption.
package server

import (
	"hash/fnv"
	"sync"
)

// Strategy selects which consumer in a group receives a message.
type Strategy interface {
	Select(members []*Consumer, key string) *Consumer
}

// RoundRobinStrategy distributes messages evenly across consumers.
type RoundRobinStrategy struct {
	idx uint64
	mu  sync.Mutex
}

// Select picks the next consumer in round-robin order.
func (r *RoundRobinStrategy) Select(members []*Consumer, key string) *Consumer {
	if len(members) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.idx % uint64(len(members))
	r.idx++
	return members[i]
}

// HashStrategy routes messages to consumers by hashing the key.
type HashStrategy struct{}

// Select picks a consumer by hashing the key.
func (h *HashStrategy) Select(members []*Consumer, key string) *Consumer {
	if len(members) == 0 {
		return nil
	}
	f := fnv.New64a()
	_, _ = f.Write([]byte(key))
	idx := f.Sum64() % uint64(len(members))
	return members[idx]
}

// ConsumerGroup tracks consumers sharing a group ID for a queue.
type ConsumerGroup struct {
	id       string
	queue    string
	strategy Strategy
	members  []*Consumer
	mu       sync.RWMutex
}

// NewConsumerGroup creates a group with the given strategy name.
func NewConsumerGroup(id, queue, strategyName string) *ConsumerGroup {
	var strategy Strategy
	switch strategyName {
	case "hash":
		strategy = &HashStrategy{}
	default:
		strategy = &RoundRobinStrategy{}
	}
	return &ConsumerGroup{
		id:       id,
		queue:    queue,
		strategy: strategy,
		members:  make([]*Consumer, 0),
	}
}

// ID returns the group identifier.
func (g *ConsumerGroup) ID() string { return g.id }

// Queue returns the subscribed queue.
func (g *ConsumerGroup) Queue() string { return g.queue }

// Add appends a consumer to the group.
func (g *ConsumerGroup) Add(c *Consumer) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.members = append(g.members, c)
}

// Remove deletes a consumer by tag.
func (g *ConsumerGroup) Remove(tag string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, m := range g.members {
		if m.Tag() == tag {
			g.members = append(g.members[:i], g.members[i+1:]...)
			return true
		}
	}
	return false
}

// Members returns a snapshot of current members.
func (g *ConsumerGroup) Members() []*Consumer {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]*Consumer, len(g.members))
	copy(out, g.members)
	return out
}

// Select picks the consumer for the given routing key.
func (g *ConsumerGroup) Select(key string) *Consumer {
	return g.strategy.Select(g.Members(), key)
}

// Count returns the number of members.
func (g *ConsumerGroup) Count() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.members)
}

// StrategyName returns the strategy name for display.
func (g *ConsumerGroup) StrategyName() string {
	switch g.strategy.(type) {
	case *HashStrategy:
		return "hash"
	default:
		return "round-robin"
	}
}
