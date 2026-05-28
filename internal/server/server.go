// server.go implements the top-level gomq AMQP broker.
package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/qdongxu/gomq/internal/auth"
	"github.com/qdongxu/gomq/internal/cluster"
	"github.com/qdongxu/gomq/internal/config"
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
	tlsListener  net.Listener
	tlsCertFile  string
	tlsKeyFile   string
	tlsCAFile    string
	tlsVerify    bool
	metrics      metrics.Collector
	mu          sync.RWMutex
	wg          sync.WaitGroup
	closed      bool
	connMap     map[*Connection]struct{} // active connections
	startTime   time.Time
	discovery   *cluster.Discovery
	membership  *cluster.Membership
	gossip      *cluster.Gossip
	mirrors     *MirrorManager
	plugins     *PluginManager
	federations *FederationManager
	shovels     *ShovelManager
	aclMgr      *auth.ACLManager
	rateLimiter *RateLimiter
	backPressure *BackPressure
	cfg         *config.Config // current runtime config for hot-reload
	pipeline    *Pipeline
	workerPool  *WorkerPool
	flushSched  *FlushScheduler
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
		plugins:     NewPluginManager(),
		federations: NewFederationManager(),
		shovels:     NewShovelManager(),
		startTime:   time.Now(),
		metrics:     &metrics.NoOp{},
		cfg:         config.Default(),
		pipeline:    NewPipeline(publisher, DefaultPipelineConfig()),
		workerPool:  NewWorkerPool(0, 256),
		flushSched:  NewFlushScheduler(50 * time.Millisecond),
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

// Addr returns the TCP address the server is listening on, or nil
// if Listen has not yet been called.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	l := s.listener
	s.mu.RUnlock()
	if l == nil {
		return nil
	}
	return l.Addr()
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

// SetTLSCertPaths stores the TLS certificate file paths for later reload.
func (s *Server) SetTLSCertPaths(certFile, keyFile, caFile string, verify bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tlsCertFile = certFile
	s.tlsKeyFile = keyFile
	s.tlsCAFile = caFile
	s.tlsVerify = verify
}

// ReloadTLS validates the new certificate files.  The TLS listener itself
// must be recreated to pick up the new certificates; this method only
// updates the stored paths and returns an error if the files are missing.
func (s *Server) ReloadTLS(cfg *config.Config) error {
	if !cfg.TLS.Enabled {
		return nil
	}
	_, err := NewTLSConfig(cfg.TLS.CertFile, cfg.TLS.KeyFile, cfg.TLS.CAFile, cfg.TLS.VerifyClient)
	if err != nil {
		return fmt.Errorf("reload tls: %w", err)
	}
	s.SetTLSCertPaths(cfg.TLS.CertFile, cfg.TLS.KeyFile, cfg.TLS.CAFile, cfg.TLS.VerifyClient)
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

		// Rate limiting.
		if s.rateLimiter != nil && !s.rateLimiter.Allow() {
			_ = conn.Close()
			continue
		}

		// Backpressure check.
		if s.backPressure != nil && !s.backPressure.CanAccept() {
			_ = conn.Close()
			continue
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
	if s.pipeline != nil {
		s.pipeline.Stop()
	}
	if s.workerPool != nil {
		s.workerPool.Stop()
	}
	if s.flushSched != nil {
		s.flushSched.Stop()
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

// Pipeline returns the batching pipeline.
func (s *Server) Pipeline() *Pipeline { return s.pipeline }

// WorkerPool returns the fixed worker pool.
func (s *Server) WorkerPool() *WorkerPool { return s.workerPool }

// FlushScheduler returns the unified flush scheduler.
func (s *Server) FlushScheduler() *FlushScheduler { return s.flushSched }

// PublishViaPipeline routes a message through the batched pipeline.
func (s *Server) PublishViaPipeline(ex, rk string, msg *Message, chID uint16) {
	s.pipeline.Submit(ex, rk, msg, chID)
}

// DeliveryTracker returns the delivery tracker.
func (s *Server) DeliveryTracker() *DeliveryTracker { return s.tracker }

// Prefetch returns the prefetch controller.
func (s *Server) Prefetch() *Prefetch { return s.prefetch }

// MirrorManager returns the server's mirror manager.
func (s *Server) MirrorManager() *MirrorManager { return s.mirrors }

// PluginManager returns the server's plugin manager.
func (s *Server) PluginManager() *PluginManager { return s.plugins }

// FederationManager returns the server's federation manager.
func (s *Server) FederationManager() *FederationManager { return s.federations }

// ShovelManager returns the server's shovel manager.
func (s *Server) ShovelManager() *ShovelManager { return s.shovels }

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

// SetACLManager configures the ACL manager for the server.
func (s *Server) SetACLManager(m *auth.ACLManager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aclMgr = m
}

// ACLManager returns the current ACL manager (may be nil).
func (s *Server) ACLManager() *auth.ACLManager {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.aclMgr
}

// StartTime returns when the server was created.
func (s *Server) StartTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.startTime
}

// SetRateLimiter configures the connection rate limiter.
func (s *Server) SetRateLimiter(rl *RateLimiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rateLimiter = rl
}

// SetBackPressure configures the memory backpressure controller.
func (s *Server) SetBackPressure(bp *BackPressure) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backPressure = bp
}

// Config returns the current runtime configuration.
func (s *Server) Config() *config.Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg
}

// ReloadConfig applies a new configuration to the running server.
// Only reloadable sections are applied; non-reloadable changes are
// ignored (the caller should log warnings for them).
func (s *Server) ReloadConfig(cfg *config.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.cfg

	// ACL rules.
	if len(cfg.ACL.Rules) > 0 {
		rules := make([]auth.Rule, 0, len(cfg.ACL.Rules))
		for _, r := range cfg.ACL.Rules {
			rules = append(rules, auth.Rule{
				User:         r.User,
				VHost:        r.VHost,
				ResourceType: auth.ResourceType(r.ResourceType),
				ResourceName: r.ResourceName,
				Permission:   auth.Permission(r.Permission),
				Allow:        r.Allow,
			})
		}
		s.aclMgr = auth.NewACLManager(rules)
	} else {
		s.aclMgr = nil
	}

	// Rate limiter.
	if cfg.Limits.MaxConnectionsPerSecond > 0 {
		burst := int(cfg.Limits.MaxConnectionsPerSecond)
		if burst < 1 {
			burst = 1
		}
		s.rateLimiter = NewRateLimiter(burst, cfg.Limits.MaxConnectionsPerSecond)
	} else {
		s.rateLimiter = nil
	}

	// Backpressure.
	if cfg.Limits.BackPressureEnabled {
		s.backPressure = NewBackPressure(cfg.Limits.MemoryThresholdPercent)
	} else {
		s.backPressure = nil
	}

	// Memory settings — propagate to the message store.
	// (Store limits are updated via the store's own options.)

	s.cfg = cfg
	_ = old // old config no longer referenced
	return nil
}
