// message_store_ext.go extends MessageStore with compression and paging.
package server

import (
	"fmt"
)

// StoreOptions configures compression and paging behaviour.
type StoreOptions struct {
	CompressionThreshold int    // min payload bytes to trigger compression
	MaxInMemoryMessages  int    // max messages per queue before paging
	PageDir              string // directory for page files
}

// ExtendedMessageStore wraps MessageStore with optional compression
// and disk paging.
type ExtendedMessageStore struct {
	*MessageStore
	compressor *Compressor
	pager      *PageManager
	opts       StoreOptions
}

// NewExtendedMessageStore creates a store with compression and paging.
func NewExtendedMessageStore(
	base *MessageStore,
	opts StoreOptions,
) *ExtendedMessageStore {
	es := &ExtendedMessageStore{
		MessageStore: base,
		opts:         opts,
	}
	if opts.CompressionThreshold > 0 {
		es.compressor = NewCompressor(opts.CompressionThreshold)
	}
	if opts.MaxInMemoryMessages > 0 && opts.PageDir != "" {
		es.pager = NewPageManager(opts.PageDir)
	}
	return es
}

// EnqueueExt compresses (if needed) and enqueues a message.  If the
// queue exceeds the in-memory limit, the oldest half of messages are
// paged to disk.
func (es *ExtendedMessageStore) EnqueueExt(
	queueName string,
	msg *Message,
) error {
	if es.compressor != nil && len(msg.Payload()) >= es.opts.CompressionThreshold {
		compressed, err := es.compressor.Compress(msg.Payload())
		if err != nil {
			return fmt.Errorf("compress: %w", err)
		}
		// Wrap payload in a new message to preserve metadata.
		if len(compressed) < len(msg.Payload()) {
			msg = NewMessage(compressed, msg.Properties())
			msg.SetDeliveryTag(msg.DeliveryTag())
			msg.SetEnqueuedAt(msg.EnqueuedAt())
			msg.SetRoutingMeta(msg.Exchange(), msg.RoutingKey())
		}
	}

	es.Enqueue(queueName, msg)

	// Check paging threshold.
	if es.pager != nil &&
		es.Len(queueName) > es.opts.MaxInMemoryMessages {
		if err := es.pageOldest(queueName); err != nil {
			return err
		}
	}
	return nil
}

// DequeueExt dequeues a message and decompresses if needed.
func (es *ExtendedMessageStore) DequeueExt(
	queueName string,
) (*Message, bool, error) {
	msg, ok := es.Dequeue(queueName)
	if !ok {
		return nil, false, nil
	}

	// Attempt decompression — if it fails, treat as uncompressed.
	if es.compressor != nil {
		decompressed, err := es.compressor.Decompress(msg.Payload())
		if err == nil && len(decompressed) > len(msg.Payload()) {
			msg = NewMessage(decompressed, msg.Properties())
			msg.SetDeliveryTag(msg.DeliveryTag())
			msg.SetEnqueuedAt(msg.EnqueuedAt())
			msg.SetRoutingMeta(msg.Exchange(), msg.RoutingKey())
		}
	}
	return msg, true, nil
}

// pageOldest flushes the oldest half of a queue to disk.
func (es *ExtendedMessageStore) pageOldest(queueName string) error {
	es.mu.Lock()
	q := es.queues[queueName]
	n := len(q)
	if n <= es.opts.MaxInMemoryMessages {
		es.mu.Unlock()
		return nil
	}
	// Move oldest half to a page.
	split := n / 2
	toPage := make([]*Message, split)
	copy(toPage, q[:split])
	// Keep newer half in memory.
	newQ := make([]*Message, n-split)
	copy(newQ, q[split:])
	es.queues[queueName] = newQ
	es.mu.Unlock()

	_, err := es.pager.Flush(queueName, toPage)
	return err
}

// LoadPage restores messages from a page file into the queue.
func (es *ExtendedMessageStore) LoadPage(
	queueName string,
	pagePath string,
) error {
	if es.pager == nil {
		return fmt.Errorf("paging not enabled")
	}
	msgs, err := es.pager.Load(pagePath)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		es.Enqueue(queueName, m)
	}
	return nil
}

// SetOptions updates store options at runtime (for tests and reconfig).
func (es *ExtendedMessageStore) SetOptions(opts StoreOptions) {
	es.opts = opts
	if opts.CompressionThreshold > 0 {
		es.compressor = NewCompressor(opts.CompressionThreshold)
	} else {
		es.compressor = nil
	}
	if opts.MaxInMemoryMessages > 0 && opts.PageDir != "" {
		es.pager = NewPageManager(opts.PageDir)
	} else {
		es.pager = nil
	}
}
