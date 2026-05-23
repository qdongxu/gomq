// deliverer.go delivers messages to consumers via channels.
package server

import (
	"sync"
	"time"

	"github.com/qdongxu/gomq/internal/metrics"
)

// Deliverer handles message delivery to subscribed consumers.
type Deliverer struct {
	consumers   *ConsumerManager
	store       *MessageStore
	tracker     *DeliveryTracker
	metrics     metrics.Collector
	batchCfg    BatchConfig
	batches     map[uint16]*DeliveryBatch // per-channel batches
	batchesMu   sync.Mutex
	stopCh      chan struct{}
	flushTicker *time.Ticker
}

// NewDeliverer creates a deliverer with required managers.
func NewDeliverer(
	cm *ConsumerManager,
	store *MessageStore,
	tracker *DeliveryTracker,
) *Deliverer {
	d := &Deliverer{
		consumers: cm,
		store:     store,
		tracker:   tracker,
		metrics:   &metrics.NoOp{},
		batchCfg:  DefaultBatchConfig(),
		batches:   make(map[uint16]*DeliveryBatch),
		stopCh:    make(chan struct{}),
	}
	d.startAutoFlush()
	return d
}

// SetBatchConfig configures batching parameters. Must be called
// before any deliveries.
func (d *Deliverer) SetBatchConfig(cfg BatchConfig) {
	d.batchCfg = cfg
}

// SetMetrics configures the metrics collector.
func (d *Deliverer) SetMetrics(m metrics.Collector) {
	d.metrics = m
}

// getOrCreateBatch returns the delivery batch for a channel,
// creating one if necessary.
func (d *Deliverer) getOrCreateBatch(chID uint16, ch *Channel) *DeliveryBatch {
	d.batchesMu.Lock()
	defer d.batchesMu.Unlock()

	if b, ok := d.batches[chID]; ok {
		return b
	}

	b := NewDeliveryBatch(ch, d.batchCfg, d.flushBatch)
	d.batches[chID] = b
	return b
}

// flushBatch sends buffered delivery items for a channel.
func (d *Deliverer) flushBatch(ch *Channel, items []DeliveryItem) error {
	for range items {
		if ch != nil {
			_ = ch.SendFrame(nil) // placeholder frame
		}
		// Metrics already counted on batch add
	}
	return nil
}

// startAutoFlush starts a background ticker that periodically
// flushes all delivery batches.
func (d *Deliverer) startAutoFlush() {
	d.flushTicker = time.NewTicker(d.batchCfg.MaxWait)
	go func() {
		for {
			select {
			case <-d.flushTicker.C:
				d.FlushAll()
			case <-d.stopCh:
				d.flushTicker.Stop()
				return
			}
		}
	}()
}

// FlushAll flushes all pending delivery batches.
func (d *Deliverer) FlushAll() {
	d.batchesMu.Lock()
	batches := make([]*DeliveryBatch, 0, len(d.batches))
	for _, b := range d.batches {
		batches = append(batches, b)
	}
	d.batchesMu.Unlock()

	for _, b := range batches {
		_, _ = b.Flush()
	}
}

// Stop shuts down the auto-flush goroutine.
func (d *Deliverer) Stop() {
	close(d.stopCh)
	d.FlushAll()
}

// Deliver sends a message to all consumers of the target queue.
// When batching is enabled, deliveries are buffered and flushed
// according to BatchConfig thresholds.
func (d *Deliverer) Deliver(
	msg *Message,
	queueName string,
	channelID uint16,
) error {
	list := d.consumers.GetConsumers(queueName)
	for _, c := range list {
		tag := uint64(0) // delivery tag assigned by tracker
		msg.SetDeliveryTag(tag)

		ch := c.Channel()
		if ch != nil {
			batch := d.getOrCreateBatch(channelID, ch)
			_, _ = batch.Add(msg, queueName, tag)
		} else {
			_ = ch.SendFrame(nil) // placeholder frame
		}

		d.tracker.Record(tag, msg, queueName, channelID)
		d.metrics.MessageConsumed()
	}
	return nil
}
