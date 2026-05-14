// channel.go manages a single AMQP channel within a connection.
package server

import (
	"sync"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// ChanState represents the lifecycle of an AMQP channel.
type ChanState int

const (
	ChanOpening ChanState = iota
	ChanOpen
	ChanFlow
	ChanClosing
	ChanClosed
)

// Channel is a lightweight sub-connection for AMQP command multiplexing.
type Channel struct {
	id        uint16
	conn      *Connection
	state     ChanState
	mu        sync.RWMutex
	flowOn    bool
	prefetch  *Prefetch
	flowCtrl  *FlowController
}

// NewChannel creates a channel with the given ID on a connection.
func NewChannel(id uint16, conn *Connection) *Channel {
	return &Channel{
		id:     id,
		conn:   conn,
		state:  ChanOpening,
		flowOn: true,
	}
}

// ID returns the channel number.
func (ch *Channel) ID() uint16 {
	return ch.id
}

// State returns the current channel state safely.
func (ch *Channel) State() ChanState {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return ch.state
}

// setState updates the channel state safely.
func (ch *Channel) setState(s ChanState) {
	ch.mu.Lock()
	ch.state = s
	ch.mu.Unlock()
}

// Open transitions the channel to Open state.
func (ch *Channel) Open() {
	ch.setState(ChanOpen)
}

// Close transitions the channel to Closing then Closed.
func (ch *Channel) Close() {
	ch.setState(ChanClosing)
	ch.setState(ChanClosed)
}

// SendFrame sends a frame on this channel through the connection.
func (ch *Channel) SendFrame(f *amqp091.Frame) error {
	f.Channel = ch.id
	return ch.conn.sendFrame(f)
}

// SetFlow sets the channel flow state (true = active, false = paused).
func (ch *Channel) SetFlow(on bool) {
	ch.mu.Lock()
	ch.flowOn = on
	ch.mu.Unlock()
}

// FlowActive reports whether the channel is accepting content frames.
func (ch *Channel) FlowActive() bool {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	if !ch.flowOn || ch.state != ChanOpen {
		return false
	}
	if ch.flowCtrl != nil && !ch.flowCtrl.IsChannelActive(ch.id) {
		return false
	}
	return true
}

// SetPrefetch attaches a prefetch limiter to the channel.
func (ch *Channel) SetPrefetch(p *Prefetch) {
	ch.mu.Lock()
	ch.prefetch = p
	ch.mu.Unlock()
}

// SetFlowController attaches a server-side flow controller.
func (ch *Channel) SetFlowController(fc *FlowController) {
	ch.mu.Lock()
	ch.flowCtrl = fc
	ch.mu.Unlock()
}
