// flusher.go — unified flush scheduler that consolidates per-channel
// flush events into a single ticker-driven loop.
package server

import (
	"context"
	"sync"
	"time"
)

// FlushScheduler merges flush events from multiple channels and
// executes them on a shared ticker, reducing timer goroutine
// overhead from O(channels) to O(1).
type FlushScheduler struct {
	ticker    *time.Ticker
	channels  map[uint16]*Channel
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewFlushScheduler creates a scheduler with the given flush interval.
func NewFlushScheduler(interval time.Duration) *FlushScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	fs := &FlushScheduler{
		ticker:   time.NewTicker(interval),
		channels: make(map[uint16]*Channel),
		ctx:      ctx,
		cancel:   cancel,
	}
	go fs.loop()
	return fs
}

// Register adds a channel to the flush set.
func (fs *FlushScheduler) Register(chID uint16, ch *Channel) {
	fs.mu.Lock()
	fs.channels[chID] = ch
	fs.mu.Unlock()
}

// Unregister removes a channel from the flush set.
func (fs *FlushScheduler) Unregister(chID uint16) {
	fs.mu.Lock()
	delete(fs.channels, chID)
	fs.mu.Unlock()
}

// Stop shuts down the scheduler.
func (fs *FlushScheduler) Stop() {
	fs.cancel()
	fs.ticker.Stop()
}

// loop runs the shared ticker and flushes all registered channels.
func (fs *FlushScheduler) loop() {
	for {
		select {
		case <-fs.ticker.C:
			fs.flushAll()
		case <-fs.ctx.Done():
			return
		}
	}
}

// flushAll snapshots the channel map and flushes each one.
func (fs *FlushScheduler) flushAll() {
	fs.mu.RLock()
	channels := make([]*Channel, 0, len(fs.channels))
	for _, ch := range fs.channels {
		channels = append(channels, ch)
	}
	fs.mu.RUnlock()

	for _, ch := range channels {
		if ch != nil {
			_ = ch.SendFrame(nil) // trigger channel flush
		}
	}
}
