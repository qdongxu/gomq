// batch.go implements message batching for publish and delivery paths
// to reduce syscall overhead and frame encoding costs.
package server

import (
	"sync"
	"time"
)

// BatchConfig controls flush behaviour.
type BatchConfig struct {
	MaxSize     int           // flush after this many messages
	MaxWait     time.Duration // flush after this duration
	MaxBytes    int           // flush if accumulated bytes exceed this
}

// DefaultBatchConfig returns a sensible production default.
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		MaxSize:  64,
		MaxWait:  10 * time.Millisecond,
		MaxBytes: 1 << 20, // 1 MiB
	}
}

// PublishBatch buffers messages before routing to reduce per-message
// overhead. Not safe for concurrent use — callers should synchronise.
type PublishBatch struct {
	cfg      BatchConfig
	msgs     []*Message
	ex       string
	rk       string
	chID     uint16
	bytes    int
	flushFn  func([]*Message, string, string, uint16) error
}

// NewPublishBatch creates a batch with the given config and flush
// callback. The callback is invoked when the batch is flushed.
func NewPublishBatch(
	cfg BatchConfig,
	flushFn func([]*Message, string, string, uint16) error,
) *PublishBatch {
	return &PublishBatch{
		cfg:     cfg,
		flushFn: flushFn,
		msgs:    make([]*Message, 0, cfg.MaxSize),
	}
}

// Add appends a message to the batch and flushes if thresholds are
// exceeded. Returns the number of messages flushed (0 if none).
func (b *PublishBatch) Add(
	msg *Message, exchange, routingKey string, chID uint16,
) (int, error) {
	if len(b.msgs) == 0 {
		b.ex = exchange
		b.rk = routingKey
		b.chID = chID
	}
	b.msgs = append(b.msgs, msg)
	b.bytes += len(msg.Payload())

	if len(b.msgs) >= b.cfg.MaxSize ||
		b.bytes >= b.cfg.MaxBytes {
		return b.Flush()
	}
	return 0, nil
}

// Flush sends all buffered messages via the flush callback and
// resets the batch. Returns the number of messages flushed.
func (b *PublishBatch) Flush() (int, error) {
	n := len(b.msgs)
	if n == 0 {
		return 0, nil
	}
	err := b.flushFn(b.msgs, b.ex, b.rk, b.chID)
	b.msgs = b.msgs[:0]
	b.bytes = 0
	return n, err
}

// Len returns the number of buffered messages.
func (b *PublishBatch) Len() int {
	return len(b.msgs)
}

// AutoFlush periodically flushes the batch on a timer. Call once
// after creating the batch; stops when the batch is abandoned.
func (b *PublishBatch) AutoFlush(stopCh <-chan struct{}) {
	ticker := time.NewTicker(b.cfg.MaxWait)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_, _ = b.Flush()
		case <-stopCh:
			_, _ = b.Flush()
			return
		}
	}
}

// DeliveryBatch buffers outbound frames to consumers for coalesced
// writes. Reduces per-message frame header overhead on the wire.
type DeliveryBatch struct {
	cfg     BatchConfig
	items   []DeliveryItem
	ch      *Channel
	flushFn func(*Channel, []DeliveryItem) error
	mu      sync.Mutex
}

// DeliveryItem holds one message delivery record.
type DeliveryItem struct {
	Msg       *Message
	QueueName string
	Tag       uint64
}

// NewDeliveryBatch creates a delivery batch for a single channel.
func NewDeliveryBatch(
	ch *Channel,
	cfg BatchConfig,
	flushFn func(*Channel, []DeliveryItem) error,
) *DeliveryBatch {
	return &DeliveryBatch{
		cfg:     cfg,
		ch:      ch,
		flushFn: flushFn,
		items:   make([]DeliveryItem, 0, cfg.MaxSize),
	}
}

// Add appends a delivery item and flushes if thresholds exceeded.
func (b *DeliveryBatch) Add(
	msg *Message, queueName string, tag uint64,
) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.items = append(b.items, DeliveryItem{
		Msg:       msg,
		QueueName: queueName,
		Tag:       tag,
	})

	if len(b.items) >= b.cfg.MaxSize {
		return b.flushLocked()
	}
	return 0, nil
}

// Len returns the number of buffered delivery items.
func (b *DeliveryBatch) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

// Flush sends all buffered delivery items and resets the batch.
func (b *DeliveryBatch) Flush() (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushLocked()
}

func (b *DeliveryBatch) flushLocked() (int, error) {
	n := len(b.items)
	if n == 0 {
		return 0, nil
	}
	err := b.flushFn(b.ch, b.items)
	b.items = b.items[:0]
	return n, err
}
