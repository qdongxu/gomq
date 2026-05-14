// heartbeat.go sends periodic heartbeat frames and detects peer silence.
package server

import (
	"sync"
	"time"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// HeartbeatMonitor sends heartbeat frames and closes the connection if
// no frame is received within two intervals.
type HeartbeatMonitor struct {
	interval   time.Duration
	sendTicker *time.Ticker
	lastRecv   time.Time
	mu         sync.RWMutex
	conn       *Connection
	stopCh     chan struct{}
	stopOnce   sync.Once
}

// NewHeartbeatMonitor creates a monitor for the given connection and
// interval in seconds.
func NewHeartbeatMonitor(
	conn *Connection,
	interval int,
) *HeartbeatMonitor {
	return &HeartbeatMonitor{
		interval: time.Duration(interval) * time.Second,
		conn:     conn,
		stopCh:   make(chan struct{}),
	}
}

// Start begins sending heartbeat frames and watching for peer silence.
func (h *HeartbeatMonitor) Start() {
	h.mu.Lock()
	h.lastRecv = time.Now()
	h.sendTicker = time.NewTicker(h.interval)
	h.mu.Unlock()

	go h.loop()
}

// Stop halts the heartbeat goroutine safely (idempotent).
func (h *HeartbeatMonitor) Stop() {
	h.stopOnce.Do(func() {
		close(h.stopCh)
	})
}

// Expect resets the receive deadline whenever any frame arrives.
func (h *HeartbeatMonitor) Expect() {
	h.mu.Lock()
	h.lastRecv = time.Now()
	h.mu.Unlock()
}

// loop runs until stopped or the peer goes silent.
func (h *HeartbeatMonitor) loop() {
	for {
		select {
		case <-h.stopCh:
			return
		case <-h.sendTicker.C:
			if h.isPeerSilent() {
				h.conn.Close()
				return
			}
			_ = h.conn.sendHeartbeat()
		}
	}
}

// isPeerSilent reports whether two intervals have passed since the last
// received frame.
func (h *HeartbeatMonitor) isPeerSilent() bool {
	h.mu.RLock()
	elapsed := time.Since(h.lastRecv)
	h.mu.RUnlock()
	return elapsed > 2*h.interval
}

// sendHeartbeat writes a heartbeat frame to the connection.
func (c *Connection) sendHeartbeat() error {
	return c.sendFrame(
		&amqp091.Frame{
			Type:    amqp091.FrameHeartbeat,
			Channel: 0,
			Payload: []byte{},
		},
	)
}
