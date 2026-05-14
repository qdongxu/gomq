// prefetch.go enforces per-channel or global unacknowledged limits.
package server

import "sync"

// Prefetch controls how many messages may be in-flight per channel.
type Prefetch struct {
	globalLimit  *prefetchLimit
	defaultLimit *prefetchLimit
	limits       map[uint16]*prefetchLimit
	byChan       map[uint16]uint16
	byConn       uint16
	mu           sync.RWMutex
}

type prefetchLimit struct {
	count  uint16
	size   uint32
	global bool
}

// NewPrefetch creates a prefetch limiter.
func NewPrefetch() *Prefetch {
	return &Prefetch{
		limits: make(map[uint16]*prefetchLimit),
		byChan: make(map[uint16]uint16),
	}
}

// SetPrefetch updates the default limit parameters.
// When global is true the limit applies across all channels.
func (p *Prefetch) SetPrefetch(
	count, size uint16,
	global bool,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if global {
		p.globalLimit = &prefetchLimit{
			count:  count,
			size:   uint32(size),
			global: true,
		}
	} else {
		p.defaultLimit = &prefetchLimit{
			count:  count,
			size:   uint32(size),
			global: false,
		}
	}
}

// SetChannelPrefetch updates the limit for a specific channel.
// When global is true the limit becomes server-wide.
func (p *Prefetch) SetChannelPrefetch(
	channelID, count, size uint16,
	global bool,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if global {
		p.globalLimit = &prefetchLimit{
			count:  count,
			size:   uint32(size),
			global: true,
		}
		delete(p.limits, channelID)
	} else {
		p.limits[channelID] = &prefetchLimit{
			count:  count,
			size:   uint32(size),
			global: false,
		}
	}
}

// limitFor returns the effective limit for a channel.
func (p *Prefetch) limitFor(channelID uint16) *prefetchLimit {
	if p.globalLimit != nil {
		return p.globalLimit
	}
	if l, ok := p.limits[channelID]; ok {
		return l
	}
	return p.defaultLimit
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
	l := p.limitFor(channelID)
	if l == nil || (l.count == 0 && l.size == 0) {
		return true
	}
	if l.global {
		return p.byConn < l.count
	}
	return p.byChan[channelID] < l.count
}

// Current returns the current unacknowledged count for a channel.
func (p *Prefetch) Current(channelID uint16) uint16 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.byChan[channelID]
}
