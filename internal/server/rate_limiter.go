// rate_limiter.go provides a token-bucket connection rate limiter.
package server

import (
	"context"
	"sync"
	"time"
)

// RateLimiter limits the rate of new connections using a token bucket.
type RateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	max      float64
	refill   float64 // tokens per second
	last     time.Time
	waiters  []chan struct{}
}

// NewRateLimiter creates a limiter with the given max burst and refill
// rate (tokens per second).  A refillRate of 0 disables limiting.
func NewRateLimiter(maxBurst int, refillRate float64) *RateLimiter {
	return &RateLimiter{
		tokens: float64(maxBurst),
		max:    float64(maxBurst),
		refill: refillRate,
		last:   time.Now(),
	}
}

// Allow returns true if a connection is permitted right now.
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if rl.max <= 0 {
		return true
	}
	rl.refillTokens()
	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}
	return false
}

// Wait blocks until a token is available or the context is cancelled.
func (rl *RateLimiter) Wait(ctx context.Context) error {
	for {
		rl.mu.Lock()
		rl.refillTokens()
		if rl.tokens >= 1 {
			rl.tokens--
			rl.mu.Unlock()
			return nil
		}
		// Calculate wait time for next token.
		wait := time.Duration((1 - rl.tokens) / rl.refill * float64(time.Second))
		if wait < 1 {
			wait = 1
		}
		rl.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			// Retry.
		}
	}
}

// refillTokens adds tokens based on elapsed time.
func (rl *RateLimiter) refillTokens() {
	if rl.refill <= 0 {
		return
	}
	now := time.Now()
	elapsed := now.Sub(rl.last).Seconds()
	rl.tokens += elapsed * rl.refill
	if rl.tokens > rl.max {
		rl.tokens = rl.max
	}
	rl.last = now
}

// SetRate updates the limiter parameters at runtime.
func (rl *RateLimiter) SetRate(maxBurst int, refillRate float64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.max = float64(maxBurst)
	rl.refill = refillRate
	if rl.tokens > rl.max {
		rl.tokens = rl.max
	}
	// When capacity increased, fill to new max.
	if rl.tokens < rl.max {
		rl.tokens = rl.max
	}
}
