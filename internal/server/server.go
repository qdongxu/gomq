// server.go implements the top-level gomq AMQP broker.
package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/qdongxu/gomq/internal/store"
)

// Server holds all broker state and manages the TCP listener.
type Server struct {
	exchanges   *ExchangeManager
	queues      *QueueManager
	bindings    *BindingManager
	store       *MessageStore
	consumers   *ConsumerManager
	publisher   *Publisher
	deliverer   *Deliverer
	tracker     *DeliveryTracker
	prefetch    *Prefetch
	flowCtrl    *FlowController
	metaStore   store.Store
	listener    net.Listener
	mu          sync.RWMutex
	wg          sync.WaitGroup
	closed      bool
	connMap     map[*Connection]struct{} // active connections
	startTime   time.Time
}

// NewServer creates a broker with all managers initialised.
func NewServer() *Server {
	return NewServerWithStore(nil)
}

// NewServerWithStore creates a broker with an optional metadata
// store for persistence.
func NewServerWithStore(metaStore store.Store) *Server {
	ex := NewExchangeManagerWithStore(metaStore)
	qm := NewQueueManagerWithStore(metaStore)
	bm := NewBindingManagerWithStore(metaStore)
	store := NewMessageStore()
	cm := NewConsumerManager()
	tracker := NewDeliveryTracker(store)
	prefetch := NewPrefetch()
	prefetch.SetPrefetch(0, 0, false)
	flowCtrl := NewFlowController()
	deliverer := NewDeliverer(cm, store, tracker)
	publisher := NewPublisher(ex, qm, bm, store, cm, tracker)

	return &Server{
		exchanges: ex,
		queues:    qm,
		bindings:  bm,
		store:     store,
		consumers: cm,
		publisher: publisher,
		deliverer: deliverer,
		tracker:   tracker,
		prefetch:  prefetch,
		flowCtrl:  flowCtrl,
		metaStore: metaStore,
		connMap:   make(map[*Connection]struct{}),
		startTime: time.Now(),
	}
}

// RestoreFromStore loads all persisted queues, exchanges, and
// bindings from the given store into memory. Call after creating
// the server but before serving connections.
func (s *Server) RestoreFromStore(ctx context.Context) error {
	if s.metaStore == nil {
		return nil
	}

	queues, err := s.metaStore.LoadQueues(ctx)
	if err != nil {
		return fmt.Errorf("load queues: %w", err)
	}
	for _, meta := range queues {
		_, _ = s.queues.Declare(meta.Name, meta.Durable,
			meta.Exclusive, meta.AutoDelete, meta.Args, nil)
	}

	exchanges, err := s.metaStore.LoadExchanges(ctx)
	if err != nil {
		return fmt.Errorf("load exchanges: %w", err)
	}
	for _, meta := range exchanges {
		var et ExchangeType
		switch meta.Type {
		case "direct":
			et = ExchangeDirect
		case "fanout":
			et = ExchangeFanout
		case "topic":
			et = ExchangeTopic
		case "headers":
			et = ExchangeHeaders
		default:
			et = ExchangeDirect
		}
		_, _ = s.exchanges.Declare(meta.Name, et, meta.Durable,
			meta.AutoDelete, meta.Internal, meta.Args)
	}

	bindings, err := s.metaStore.LoadBindings(ctx)
	if err != nil {
		return fmt.Errorf("load bindings: %w", err)
	}
	for _, meta := range bindings {
		_, _ = s.bindings.Bind(meta.Exchange, meta.Queue,
			meta.RoutingKey, meta.Args)
	}
	return nil
}

// Listen starts a TCP listener on the given address.
func (s *Server) Listen(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %q: %w", addr, err)
	}
	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()
	return nil
}

// Serve accepts connections and runs the AMQP frame loop.
func (s *Server) Serve() error {
	s.mu.RLock()
	l := s.listener
	s.mu.RUnlock()
	if l == nil {
		return fmt.Errorf("no listener")
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			s.mu.RLock()
			closed := s.closed
			s.mu.RUnlock()
			if closed {
				return nil
			}
			return err
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// handleConn runs a single client connection.
func (s *Server) handleConn(raw net.Conn) {
	defer s.wg.Done()
	auth := NewMemoryAuthenticator()
	c := NewConnection(raw, auth, s)
	s.registerConn(c)
	defer s.unregisterConn(c)
	c.Serve()
}

func (s *Server) registerConn(c *Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connMap == nil {
		s.connMap = make(map[*Connection]struct{})
	}
	s.connMap[c] = struct{}{}
}

func (s *Server) unregisterConn(c *Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.connMap, c)
}

// ConnectionList returns a snapshot of active connections.
func (s *Server) ConnectionList() []*Connection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Connection, 0, len(s.connMap))
	for c := range s.connMap {
		out = append(out, c)
	}
	return out
}

// Shutdown closes the listener and waits for connections to finish.
func (s *Server) Shutdown() error {
	s.mu.Lock()
	s.closed = true
	l := s.listener
	s.mu.Unlock()

	if l != nil {
		_ = l.Close()
	}
	s.wg.Wait()
	return nil
}

// ExchangeManager returns the exchange manager.
func (s *Server) ExchangeManager() *ExchangeManager { return s.exchanges }

// QueueManager returns the queue manager.
func (s *Server) QueueManager() *QueueManager { return s.queues }

// BindingManager returns the binding manager.
func (s *Server) BindingManager() *BindingManager { return s.bindings }

// MessageStore returns the message store.
func (s *Server) MessageStore() *MessageStore { return s.store }

// ConsumerManager returns the consumer manager.
func (s *Server) ConsumerManager() *ConsumerManager { return s.consumers }

// Publisher returns the publisher.
func (s *Server) Publisher() *Publisher { return s.publisher }

// DeliveryTracker returns the delivery tracker.
func (s *Server) DeliveryTracker() *DeliveryTracker { return s.tracker }

// Prefetch returns the prefetch controller.
func (s *Server) Prefetch() *Prefetch { return s.prefetch }

// FlowController returns the flow controller.
func (s *Server) FlowController() *FlowController { return s.flowCtrl }

// StartTime returns when the server was created.
func (s *Server) StartTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startTime
}
