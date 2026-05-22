// server.go implements the top-level gomq AMQP broker.
package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/qdongxu/gomq/internal/cluster"
	"github.com/qdongxu/gomq/internal/metrics"
	"github.com/qdongxu/gomq/internal/store"
)

// Server holds all broker state and manages the TCP listener.
type Server struct {
	exchanges   *ExchangeManager
	queues      *QueueManager
	bindings    *BindingManager
	e2eBindings *E2EBindingManager
	store       *MessageStore
	consumers   *ConsumerManager
	publisher   *Publisher
	deliverer   *Deliverer
	tracker     *DeliveryTracker
	prefetch    *Prefetch
	flowCtrl    *FlowController
	metaStore   store.Store
	listener    net.Listener
	tlsListener net.Listener
	metrics     metrics.Collector
	mu          sync.RWMutex
	wg          sync.WaitGroup
	closed      bool
	connMap     map[*Connection]struct{} // active connections
	startTime   time.Time
	discovery   *cluster.Discovery
	membership  *cluster.Membership
	gossip      *cluster.Gossip
	mirrors     *MirrorManager
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
	e2ebm := NewE2EBindingManager()
	store := NewMessageStore()
	cm := NewConsumerManager()
	tracker := NewDeliveryTracker(store)
	prefetch := NewPrefetch()
	prefetch.SetPrefetch(0, 0, false)
	flowCtrl := NewFlowController()
	deliverer := NewDeliverer(cm, store, tracker)
	publisher := NewPublisher(ex, qm, bm, e2ebm, store, cm, tracker)

	return &Server{
		exchanges:   ex,
		queues:      qm,
		bindings:    bm,
		e2eBindings: e2ebm,
		store:       store,
		consumers:   cm,
		publisher:   publisher,
		deliverer:   deliverer,
		tracker:     tracker,
		prefetch:    prefetch,
		flowCtrl:    flowCtrl,
		metaStore:   metaStore,
		connMap:     make(map[*Connection]struct{}),
		mirrors:     NewMirrorManager(),
		startTime:   time.Now(),
		metrics:     &metrics.NoOp{},
	}
}

// EnableClusterDiscovery registers the server with etcd and
// starts the gossip loop for node membership.
func (s *Server) EnableClusterDiscovery(
	client interface{ Grant(context.Context, int64) (*interface{}, error) },
	localID, localAddr string,
) error {
	// Stub: real integration requires *clientv3.Client.
	// This method is a placeholder for the wiring layer in
	// cmd/gomqd/main.go to connect an etcd client.
	_ = client
	_ = localID
	_ = localAddr
	return nil
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

// ListenTLS starts a TLS listener on the given address.
func (s *Server) ListenTLS(addr string, tlsConfig *tls.Config) error {
	l, err := tls.Listen("tcp", addr, tlsConfig)
	if err != nil {
		return fmt.Errorf("listen tls %q: %w", addr, err)
	}
	s.mu.Lock()
	s.tlsListener = l
	s.mu.Unlock()
	return nil
}

// Serve accepts connections and runs the AMQP frame loop.
func (s *Server) Serve() error {
	s.mu.RLock()
	l := s.listener
	tlsL := s.tlsListener
	s.mu.RUnlock()
	if l == nil && tlsL == nil {
		return fmt.Errorf("no listener")
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	if l != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.serveListener(l); err != nil {
				errCh <- err
			}
		}()
	}
	if tlsL != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.serveListener(tlsL); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// serveListener accepts connections from a single listener.
func (s *Server) serveListener(l net.Listener) error {
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
	s.metrics.ConnectionOpened()
}

func (s *Server) unregisterConn(c *Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.connMap, c)
	s.metrics.ConnectionClosed()
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

// Shutdown closes all listeners and waits for connections to finish.
func (s *Server) Shutdown() error {
	s.mu.Lock()
	s.closed = true
	l := s.listener
	tlsL := s.tlsListener
	s.mu.Unlock()

	if l != nil {
		_ = l.Close()
	}
	if tlsL != nil {
		_ = tlsL.Close()
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

// E2EBindingManager returns the E2E binding manager.
func (s *Server) E2EBindingManager() *E2EBindingManager { return s.e2eBindings }

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

// MirrorManager returns the server's mirror manager.
func (s *Server) MirrorManager() *MirrorManager { return s.mirrors }

// FlowController returns the flow controller.
func (s *Server) FlowController() *FlowController { return s.flowCtrl }

// SetMetrics configures the metrics collector and propagates it to all
// sub-components.
func (s *Server) SetMetrics(m metrics.Collector) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = m
	s.queues.SetMetrics(m)
	s.consumers.SetMetrics(m)
	s.publisher.SetMetrics(m)
	s.deliverer.SetMetrics(m)
}

// Metrics returns the current metrics collector.
func (s *Server) Metrics() metrics.Collector {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.metrics
}

// StartTime returns when the server was created.
func (s *Server) StartTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startTime
}
