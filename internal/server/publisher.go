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
	e2eBindings *E2EBindingManager
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
	e2ebm *E2EBindingManager,
	store *MessageStore,
	cm *ConsumerManager,
	tracker *DeliveryTracker,
) *Publisher {
	d := NewDeliverer(cm, store, tracker)
	return &Publisher{
		exchanges:   ex,
		queues:      qm,
		bindings:    bm,
		e2eBindings: e2ebm,
		store:       store,
		consumers:   cm,
		deliverer:   d,
		tracker:     tracker,
		stats:       make(map[string]*exchangeStats),
	}
}

// Publish routes a message to bound queues and delivers to consumers.
// Returns the number of queues the message was routed to.
func (p *Publisher) Publish(
	exchangeName, routingKey string,
	msg *Message,
	channelID uint16,
) (int, error) {
	n, err := p.publishInternal(exchangeName, routingKey, msg,
		channelID, true, make(map[string]bool))
	if n >= 0 && err == nil {
		p.recordStats(exchangeName, int64(n))
	}
	return n, err
}

// publishInternal performs routing with optional DLX/max-length checks
// and E2E forwarding. visited prevents exchange routing loops.
func (p *Publisher) publishInternal(
	exchangeName, routingKey string,
	msg *Message,
	channelID uint16,
	checkDLX bool,
	visited map[string]bool,
) (int, error) {
	if visited[exchangeName] {
		return 0, fmt.Errorf("exchange loop detected: %s",
			exchangeName)
	}
	visited[exchangeName] = true

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

	// Forward to destination exchanges via E2E bindings.
	if p.e2eBindings != nil {
		e2eList := p.e2eBindings.GetBindings(exchangeName)
		for _, eb := range e2eList {
			n2, err := p.publishInternal(
				eb.Destination, eb.RoutingKey, msg,
				channelID, checkDLX, visited)
			if err != nil {
				continue
			}
			queues = append(queues, make([]string, n2)...)
		}
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
		_, err := p.publishInternal(ex, rk, m, 0, false,
			make(map[string]bool))
		return err
	})
}
