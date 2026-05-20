// publisher.go coordinates end-to-end message publishing.
package server

import (
	"fmt"
	"sync"
	"time"
)

// Publisher routes published messages to queues and consumers.
type Publisher struct {
	exchanges   *ExchangeManager
	queues      *QueueManager
	bindings    *BindingManager
	store       *MessageStore
	consumers   *ConsumerManager
	deliverer   *Deliverer
	tracker     *DeliveryTracker
	stats       map[string]*exchangeStats
	statsMu     sync.RWMutex
}

// exchangeStats holds per-exchange message counters.
type exchangeStats struct {
	in  int64
	out int64
}

// NewPublisher creates a publisher with all required managers.
func NewPublisher(
	ex *ExchangeManager,
	qm *QueueManager,
	bm *BindingManager,
	store *MessageStore,
	cm *ConsumerManager,
	tracker *DeliveryTracker,
) *Publisher {
	d := NewDeliverer(cm, store, tracker)
	return &Publisher{
		exchanges: ex,
		queues:    qm,
		bindings:  bm,
		store:     store,
		consumers: cm,
		deliverer: d,
		tracker:   tracker,
		stats:     make(map[string]*exchangeStats),
	}
}

// Publish routes a message to bound queues and delivers to consumers.
// Returns the number of queues the message was routed to.
func (p *Publisher) Publish(
	exchangeName, routingKey string,
	msg *Message,
	channelID uint16,
) (int, error) {
	n, err := p.publishInternal(exchangeName, routingKey, msg, channelID, true)
	if n >= 0 && err == nil {
		p.recordStats(exchangeName, int64(n))
	}
	return n, err
}

// publishInternal performs routing with optional DLX/max-length checks.
func (p *Publisher) publishInternal(
	exchangeName, routingKey string,
	msg *Message,
	channelID uint16,
	checkDLX bool,
) (int, error) {
	ex, ok := p.exchanges.Get(exchangeName)
	if !ok {
		return 0, fmt.Errorf("exchange %q not found", exchangeName)
	}

	bindings := p.bindings.GetBindings(exchangeName)
	queues := ex.Router().Route(routingKey, msg.properties.Headers, bindings)

	for _, qn := range queues {
		q, ok := p.queues.Get(qn)
		if !ok {
			continue
		}

		// Handle max-length overflow: dead-letter oldest message.
		if checkDLX {
			maxLen := MaxLength(q.Args)
			if maxLen > 0 && p.store.Len(qn) >= maxLen {
				oldest, _ := p.store.Dequeue(qn)
				if oldest != nil {
					p.deadLetter(oldest, q.Args)
				}
			}
		}

		msg.SetEnqueuedAt(time.Now())

		// Priority-aware enqueue when queue declares x-max-priority.
		if pri := MaxPriority(q.Args); pri > 0 {
			p.store.EnqueuePriority(qn, msg)
		} else {
			p.store.Enqueue(qn, msg)
		}

		// Skip delivery if message already expired.
		if IsExpired(msg, q.Args) {
			continue
		}

		_ = p.deliverer.Deliver(msg, qn, channelID)
	}
	return len(queues), nil
}

// ExchangeStats returns the accumulated in/out counters for an exchange.
func (p *Publisher) ExchangeStats(name string) (in, out int64) {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	if s, ok := p.stats[name]; ok {
		return s.in, s.out
	}
	return 0, 0
}

func (p *Publisher) recordStats(name string, routed int64) {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	s, ok := p.stats[name]
	if !ok {
		s = &exchangeStats{}
		p.stats[name] = s
	}
	s.in++
	s.out += routed
}

// DeadLetter routes a rejected or expired message to the configured
// dead-letter exchange.
func (p *Publisher) DeadLetter(
	msg *Message,
	args map[string]interface{},
) {
	p.deadLetter(msg, args)
}

// deadLetter routes a message to the configured dead-letter exchange.
func (p *Publisher) deadLetter(
	msg *Message,
	args map[string]interface{},
) {
	_ = RouteDeadLetter(msg, args, func(ex, rk string, m *Message) error {
		_, err := p.publishInternal(ex, rk, m, 0, false)
		return err
	})
}
