// prefetch.go enforces per-channel or global unacknowledged limits.
package server

import "sync"

// Prefetch controls how many messages may be in-flight per channel.
type Prefetch struct {
	count   uint16
	size    uint32
	global  bool
	byChan  map[uint16]uint16 // current unacked count
	byConn  uint16            // global unacked count
	mu      sync.RWMutex
}

// NewPrefetch creates a prefetch limiter.
func NewPrefetch() *Prefetch {
	return &Prefetch{
		byChan: make(map[uint16]uint16),
	}
}

// SetPrefetch updates the limit parameters.
func (p *Prefetch) SetPrefetch(
	count, size uint16,
	global bool,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.count = count
	p.size = uint32(size)
	p.global = global
}

// RecordDelivery increments the unacknowledged count.
func (p *Prefetch) RecordDelivery(channelID uint16) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.byChan[channelID]++
	p.byConn++
}

// AckDelivery decrements the unacknowledged count.
func (p *Prefetch) AckDelivery(channelID uint16) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.byChan[channelID] > 0 {
		p.byChan[channelID]--
	}
	if p.byConn > 0 {
		p.byConn--
	}
}

// CanDeliver reports whether another message may be sent.
func (p *Prefetch) CanDeliver(channelID uint16) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.count == 0 && p.size == 0 {
		return true
	}
	if p.global {
		return p.byConn < p.count
	}
	return p.byChan[channelID] < p.count
}

// Current returns the current unacknowledged count for a channel.
func (p *Prefetch) Current(channelID uint16) uint16 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.byChan[channelID]
}
