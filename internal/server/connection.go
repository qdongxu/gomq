// connection.go manages a single AMQP client TCP connection.
package server

import (
	"bufio"
	"bytes"
	"net"
	"sync"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// ConnState represents the lifecycle of an AMQP connection.
type ConnState int

const (
	StateInit ConnState = iota
	StateHeader
	StateStart
	StateTune
	StateOpen
	StateClosing
	StateClosed
)

// Connection wraps a net.Conn and manages the AMQP frame lifecycle.
type Connection struct {
	raw        net.Conn
	reader     *bufio.Reader
	writer     *bufio.Writer
	state      ConnState
	mu         sync.RWMutex
	auth       Authenticator
	channelMgr *ChannelManager
	dispatcher *Dispatcher
	hb         *HeartbeatMonitor
	server     *Server
	heartbeat  int // negotiated heartbeat interval
	registry   *SimpleRegistry
}

// NewConnection creates a Connection from an accepted net.Conn.
func NewConnection(
	raw net.Conn,
	auth Authenticator,
	srv *Server,
) *Connection {
	reg := NewSimpleRegistry()
	c := &Connection{
		raw:      raw,
		reader:   bufio.NewReader(raw),
		writer:   bufio.NewWriter(raw),
		state:    StateInit,
		auth:     auth,
		server:   srv,
		registry: reg,
	}
	c.channelMgr = NewChannelManager(2048)
	c.dispatcher = NewDispatcher(reg)
	c.registerConnectionMethods()
	return c
}

// Serve runs the connection lifecycle: handshake then frame loop.
func (c *Connection) Serve() {
	defer c.Close()

	c.setState(StateHeader)
	hs := NewHandshaker(c, c.auth)
	if err := hs.Negotiate(); err != nil {
		return
	}
	c.setState(StateOpen)

	// Start heartbeat after negotiation.
	c.hb = NewHeartbeatMonitor(c, c.heartbeat)
	if c.heartbeat > 0 {
		c.hb.Start()
	}
	defer c.hb.Stop()

	// Frame loop.
	for c.State() == StateOpen {
		f, err := c.readFrame()
		if err != nil {
			return
		}
		c.hb.Expect()

		if f.Channel == 0 {
			if err := c.handleConnectionFrame(f); err != nil {
				return
			}
			continue
		}

		if err := c.handleChannelFrame(f); err != nil {
			return
		}
	}
}

// Close performs a graceful AMQP connection close.
func (c *Connection) Close() error {
	c.mu.Lock()
	if c.state >= StateClosing {
		c.mu.Unlock()
		return nil
	}
	c.state = StateClosing
	c.mu.Unlock()

	if c.hb != nil {
		c.hb.Stop()
	}
	if c.channelMgr != nil {
		c.channelMgr.CloseAll()
	}

	// Send Connection.Close if we are in Open state.
	if c.State() == StateOpen {
		enc := amqp091.NewEncoder()
		enc.WriteUint16(10) // Connection class
		enc.WriteUint16(50) // Close method
		enc.WriteUint16(200) // reply-code
		enc.WriteString("normal shutdown")
		enc.WriteUint16(0) // class-id
		enc.WriteUint16(0) // method-id
		_ = c.sendMethodFrame(0, enc.Bytes())
	}

	err := c.raw.Close()
	c.setState(StateClosed)
	return err
}

// State returns the current connection state safely.
func (c *Connection) State() ConnState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// setState updates the connection state safely.
func (c *Connection) setState(s ConnState) {
	c.mu.Lock()
	c.state = s
	c.mu.Unlock()
}

// sendFrame writes a frame to the wire with mutex protection.
func (c *Connection) sendFrame(f *amqp091.Frame) error {
	enc := amqp091.NewEncoder()
	if err := enc.EncodeFrame(f); err != nil {
		return err
	}
	c.mu.Lock()
	_, err := c.writer.Write(enc.Bytes())
	if err != nil {
		c.mu.Unlock()
		return err
	}
	err = c.writer.Flush()
	c.mu.Unlock()
	return err
}

// sendMethodFrame sends a method frame on the given channel.
func (c *Connection) sendMethodFrame(
	ch uint16,
	payload []byte,
) error {
	return c.sendFrame(
		&amqp091.Frame{
			Type:    amqp091.FrameMethod,
			Channel: ch,
			Payload: payload,
		},
	)
}

// readFrame reads the next frame from the connection.
func (c *Connection) readFrame() (*amqp091.Frame, error) {
	fr := amqp091.NewFramer(c.reader, amqp091.FrameMaxDefault)
	return fr.NextFrame()
}

// handleConnectionFrame processes channel-0 control frames.
func (c *Connection) handleConnectionFrame(
	f *amqp091.Frame,
) error {
	if f.Type != amqp091.FrameMethod {
		return nil // ignore non-method frames on channel 0
	}
	dec := amqp091.NewDecoder(bytes.NewReader(f.Payload))
	classID, _ := dec.ReadUint16()
	methodID, _ := dec.ReadUint16()

	if classID == 10 && methodID == 50 {
		// Connection.Close requested by client.
		return c.Close()
	}
	return nil
}

// handleChannelFrame dispatches a non-zero channel frame.
func (c *Connection) handleChannelFrame(
	f *amqp091.Frame,
) error {
	ch, ok := c.channelMgr.Get(f.Channel)
	if !ok {
		// Channel not open; ignore or send Channel.Close.
		return nil
	}
	return c.dispatcher.Dispatch(ch, f)
}

// registerConnectionMethods registers handlers for Connection class.
func (c *Connection) registerConnectionMethods() {
	// Connection.Close is handled inline in handleConnectionFrame.
	// No other Connection methods dispatched through channels.
}

// Server is a placeholder for the top-level server (filled in later).
type Server struct{}
