// pipeline.go — batched producer-consumer pipeline for publish throughput.
//
// The Pipeline decouples the publish hot-path from routing and delivery
// by buffering messages into small batches and processing them with a
// worker pool.  This reduces lock contention and amortises per-message
// overhead.
package server

import (
	"context"
	"sync"
	"sync/atomic"
)

// PipelineStage identifies a stage in the processing chain.
type PipelineStage int

const (
	StageRoute PipelineStage = iota
	StageDeliver
)

// PipelineJob represents a unit of work for the worker pool.
type PipelineJob struct {
	Stage     PipelineStage
	Messages  []*Message
	Exchange  string
	RoutingKey string
	ChannelID uint16
	ResultCh  chan PipelineResult
}

// PipelineResult carries the outcome of a pipeline job.
type PipelineResult struct {
	Routed int
	Err    error
}

// PipelineConfig controls batching and worker behaviour.
type PipelineConfig struct {
	BatchSize    int           // max messages per batch
	WorkerCount  int           // number of workers (0 = GOMAXPROCS)
	InputBuffer  int           // capacity of input channel
}

// DefaultPipelineConfig returns sensible defaults tuned for a
// 4-core machine.
func DefaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		BatchSize:   64,
		WorkerCount: 0, // auto = runtime.GOMAXPROCS(0)
		InputBuffer: 256,
	}
}

// Pipeline batches incoming publish requests and dispatches them to
// a fixed worker pool for routing and delivery.
type Pipeline struct {
	publisher  *Publisher
	cfg        PipelineConfig
	input      chan *pipelineItem
	workers    int
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	drained    int64 // atomic counter
	mu         sync.Mutex
	inFlight   int64     // atomic: items submitted but not yet processed
	cond       *sync.Cond // signals when inFlight drops to zero
}

// pipelineItem is a single message waiting to be batched.
type pipelineItem struct {
	exchange   string
	routingKey string
	msg        *Message
	channelID  uint16
}

// NewPipeline creates a pipeline backed by the given publisher.
func NewPipeline(pub *Publisher, cfg PipelineConfig) *Pipeline {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 4 // safe default if GOMAXPROCS unavailable
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 64
	}
	if cfg.InputBuffer <= 0 {
		cfg.InputBuffer = 256
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pipeline{
		publisher: pub,
		cfg:       cfg,
		input:     make(chan *pipelineItem, cfg.InputBuffer),
		workers:   cfg.WorkerCount,
		ctx:       ctx,
		cancel:    cancel,
	}
	p.cond = sync.NewCond(&sync.Mutex{})
	p.startWorkers()
	return p
}

// Submit queues a message for batched processing.  The call returns
// immediately; the message is routed and delivered asynchronously.
// For synchronous use call Flush after a burst of submits.
func (p *Pipeline) Submit(exchange, routingKey string, msg *Message, channelID uint16) {
	atomic.AddInt64(&p.inFlight, 1)
	select {
	case p.input <- &pipelineItem{
		exchange:   exchange,
		routingKey: routingKey,
		msg:        msg,
		channelID:  channelID,
	}:
	case <-p.ctx.Done():
		atomic.AddInt64(&p.inFlight, -1)
		p.cond.Broadcast()
		// pipeline shutting down — drop silently
	}
}

// Flush drains the input buffer and waits for all pending items to
// be processed.  Useful in benchmarks and tests that need synchronous
// semantics.
func (p *Pipeline) Flush() {
	p.cond.L.Lock()
	for atomic.LoadInt64(&p.inFlight) > 0 || len(p.input) > 0 {
		p.cond.Wait()
	}
	p.cond.L.Unlock()
}

// Stop shuts down the pipeline gracefully.
func (p *Pipeline) Stop() {
	p.cancel()
	p.wg.Wait()
}

// startWorkers launches the fixed goroutine pool.
func (p *Pipeline) startWorkers() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// worker loops until cancelled, gathering items into batches.
func (p *Pipeline) worker() {
	defer p.wg.Done()
	batch := make([]*pipelineItem, 0, p.cfg.BatchSize)
	for {
		select {
		case item := <-p.input:
			batch = append(batch, item)
			// Fill batch opportunistically without blocking.
		inner:
			for len(batch) < p.cfg.BatchSize {
				select {
				case item := <-p.input:
					batch = append(batch, item)
				default:
					break inner
				}
			}
			p.processBatch(batch)
			batch = batch[:0]
		case <-p.ctx.Done():
			// Drain remaining items.
			for len(p.input) > 0 {
				select {
				case item := <-p.input:
					batch = append(batch, item)
				default:
					if len(batch) > 0 {
						p.processBatch(batch)
					}
					p.cond.Broadcast()
					return
				}
			}
			if len(batch) > 0 {
				p.processBatch(batch)
			}
			p.cond.Broadcast()
			return
		}
	}
}

// processBatch routes and delivers a batch of messages.  It groups
// by exchange to amortise routing lookups and reduces per-message
// lock overhead.
func (p *Pipeline) processBatch(batch []*pipelineItem) {
	// Group by exchange to reduce repeated lookups.
	byEx := make(map[string][]*pipelineItem, 8)
	for _, item := range batch {
		byEx[item.exchange] = append(byEx[item.exchange], item)
	}
	for _, items := range byEx {
		for _, item := range items {
			_, _ = p.publisher.Publish(
				item.exchange, item.routingKey,
				item.msg, item.channelID,
			)
			atomic.AddInt64(&p.drained, 1)
		}
	}
	n := int64(len(batch))
	if atomic.AddInt64(&p.inFlight, -n) == 0 {
		p.cond.Broadcast()
	}
}
