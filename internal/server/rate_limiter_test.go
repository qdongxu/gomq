// rate_limiter_test.go tests the token-bucket rate limiter.
package server

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiterAllowWithinBurst(t *testing.T) {
	rl := NewRateLimiter(5, 0) // 5 burst, no refill
	for i := 0; i < 5; i++ {
		if !rl.Allow() {
			t.Fatalf("expected allow at %d", i)
		}
	}
	if rl.Allow() {
		t.Fatalf("expected deny after burst exhausted")
	}
}

func TestRateLimiterDisabled(t *testing.T) {
	rl := NewRateLimiter(0, 0)
	if !rl.Allow() {
		t.Fatalf("disabled limiter should always allow")
	}
}

func TestRateLimiterWait(t *testing.T) {
	rl := NewRateLimiter(1, 10) // 1 burst, 10/sec refill
	if !rl.Allow() {
		t.Fatalf("expected initial allow")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := rl.Wait(ctx)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatalf("wait returned too fast")
	}
}

func TestRateLimiterWaitCancelled(t *testing.T) {
	rl := NewRateLimiter(0, 0.1) // very slow refill
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := rl.Wait(ctx)
	if err != context.Canceled {
		t.Fatalf("expected cancelled, got %v", err)
	}
}

func TestRateLimiterSetRate(t *testing.T) {
	rl := NewRateLimiter(1, 0)
	if !rl.Allow() {
		t.Fatalf("expected allow")
	}
	if rl.Allow() {
		t.Fatalf("expected deny")
	}
	rl.SetRate(3, 0)
	for i := 0; i < 3; i++ {
		if !rl.Allow() {
			t.Fatalf("expected allow after setRate at %d", i)
		}
	}
}
