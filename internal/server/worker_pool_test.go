// worker_pool_test.go — unit tests for the fixed goroutine pool.
package server

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool_SubmitAndExecute(t *testing.T) {
	wp := NewWorkerPool(2, 8)
	defer wp.Stop()

	var executed int64
	wp.Submit(func() { atomic.AddInt64(&executed, 1) })
	wp.Submit(func() { atomic.AddInt64(&executed, 1) })

	// Allow workers to pick up jobs.
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt64(&executed) != 2 {
		t.Fatalf("expected 2 executions, got %d", executed)
	}
}

func TestWorkerPool_ManyJobs(t *testing.T) {
	wp := NewWorkerPool(4, 64)
	defer wp.Stop()

	const n = 100
	var executed int64
	for i := 0; i < n; i++ {
		wp.Submit(func() { atomic.AddInt64(&executed, 1) })
	}

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt64(&executed) != n {
		t.Fatalf("expected %d executions, got %d", n, executed)
	}
}

func TestWorkerPool_StopDrains(t *testing.T) {
	wp := NewWorkerPool(1, 4)

	var executed int64
	for i := 0; i < 5; i++ {
		wp.Submit(func() { atomic.AddInt64(&executed, 1) })
	}
	wp.Stop()

	if atomic.LoadInt64(&executed) != 5 {
		t.Fatalf("expected 5 executions after stop, got %d", executed)
	}
}
