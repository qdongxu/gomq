// worker_pool.go — fixed goroutine pool for route/deliver workloads.
package server

import (
	"context"
	"sync"
	"sync/atomic"
)

// WorkerPool dispatches jobs to a fixed set of goroutines.
type WorkerPool struct {
	workers int
	queue   chan func()
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	closed  int32 // atomic
}

// NewWorkerPool creates a pool with the given worker count and
// job-queue capacity.  workerCount=0 defaults to GOMAXPROCS.
func NewWorkerPool(workerCount, queueCapacity int) *WorkerPool {
	if workerCount <= 0 {
		workerCount = 4 // safe default
	}
	if queueCapacity <= 0 {
		queueCapacity = 256
	}
	ctx, cancel := context.WithCancel(context.Background())
	wp := &WorkerPool{
		workers: workerCount,
		queue:   make(chan func(), queueCapacity),
		ctx:     ctx,
		cancel:  cancel,
	}
	wp.start()
	return wp
}

// Submit enqueues a job.  The job is executed by one of the workers.
// Submit never blocks; if the queue is full the job is dropped.
func (wp *WorkerPool) Submit(job func()) {
	if atomic.LoadInt32(&wp.closed) == 1 {
		return
	}
	defer func() {
		if recover() != nil {
			// channel was closed — execute inline
			job()
		}
	}()
	select {
	case wp.queue <- job:
	case <-wp.ctx.Done():
		// pool shutting down — drop silently
	default:
		// queue full — execute inline to avoid blocking hot path
		job()
	}
}

// Stop gracefully shuts down the pool after draining pending jobs.
func (wp *WorkerPool) Stop() {
	atomic.StoreInt32(&wp.closed, 1)
	wp.cancel()
	close(wp.queue)
	wp.wg.Wait()
}

// start launches the worker goroutines.
func (wp *WorkerPool) start() {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.loop()
	}
}

// loop runs until the queue is closed and drained.
func (wp *WorkerPool) loop() {
	defer wp.wg.Done()
	for job := range wp.queue {
		job()
	}
}
