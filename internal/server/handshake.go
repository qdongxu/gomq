// handshake.go implements the AMQP 0-9-1 connection startup sequence.
package server

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"time"

	saslauth "github.com/qdongxu/gomq/internal/auth"
	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// protocolHeader is the 8-byte AMQP 0-9-1 greeting.
var protocolHeader = []byte{
	'A', 'M', 'Q', 'P', 0x00, 0x00, 0x09, 0x01,
}

// Handshaker manages the connection startup negotiation.
type Handshaker struct {
	conn   *Connection
	auth   Authenticator
	sasl   *saslauth.Registry
	tune   *amqpTuneParams
}

type amqpTuneParams struct {
	channelMax uint16
	frameMax   uint32
	heartbeat  uint16
}

// NewHandshaker creates a handshaker for the given connection and
// authenticator.  It automatically registers PLAIN (via the existing
// Authenticator), AMQPLAIN, and EXTERNAL SASL mechanisms.
func NewHandshaker(
	conn *Connection,
	auth Authenticator,
) *Handshaker {
	reg := saslauth.NewSASLRegistry()
	reg.Register(saslauth.NewAMQPLAIN())
	reg.Register(saslauth.NewEXTERNAL())
	return &Handshaker{
		conn: conn,
		auth: auth,
		sasl: reg,
		tune: &amqpTuneParams{
			channelMax: 2048,
			frameMax:   131072,
			heartbeat:  60,
		},
	}
}

// Negotiate performs the full AMQP connection startup.
func (h *Handshaker) Negotiate() error {
	if err := h.readProtocolHeader(); err != nil {
		return fmt.Errorf("protocol header: %w", err)
	}
	if err := h.sendStart(); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	if err := h.readStartOk(); err != nil {
		return fmt.Errorf("start-ok: %w", err)
	}
	if err := h.sendTune(); err != nil {
		return fmt.Errorf("tune: %w", err)
	}
	if err := h.readTuneOk(); err != nil {
		return fmt.Errorf("tune-ok: %w", err)
	}
	h.conn.heartbeat = int(h.tune.heartbeat)
	if err := h.sendOpen(); err != nil {
		return fmt.Errorf("open: %w", err)
	}
	if err := h.readOpenOk(); err != nil {
		return fmt.Errorf("open-ok: %w", err)
	}
	return nil
}

// readProtocolHeader reads and validates the 8-byte client header.
func (h *Handshaker) readProtocolHeader() error {
	h.conn.setDeadline(10 * time.Second)
	defer h.conn.clearDeadline()

	buf := make([]byte, 8)
	if _, err := io.ReadFull(h.conn.raw, buf); err != nil {
		return err
	}
	if buf[0] != 'A' {
		return fmt.Errorf("invalid protocol header %q", buf)
	}
	return nil
}

// sendStart sends Connection.Start with registered SASL mechanisms.
func (h *Handshaker) sendStart() error {
	enc := amqp091.NewEncoder()
	enc.WriteUint16(10)  // Connection class
	enc.WriteUint16(10)  // Start method
	enc.WriteUint8(0)    // version-major
	enc.WriteUint8(9)    // version-minor
	enc.WriteTable(map[string]interface{}{
		"product": "gomq",
		"version": "0.1.0",
	})
	enc.WriteString(h.sasl.Names())
	enc.WriteString("en_US")

	return h.conn.sendMethodFrame(0, enc.Bytes())
}

// readStartOk reads Connection.Start-Ok and validates credentials via
// the chosen SASL mechanism.
func (h *Handshaker) readStartOk() error {
	f, err := h.conn.readFrame()
	if err != nil {
		return err
	}
	if f.Type != amqp091.FrameMethod {
		return fmt.Errorf("expected method frame, got %d", f.Type)
	}
	// Decode class-id, method-id
	dec := amqp091.NewDecoder(bytes.NewReader(f.Payload))
	classID, _ := dec.ReadUint16()
	methodID, _ := dec.ReadUint16()
	if classID != 10 || methodID != 11 {
		return fmt.Errorf("unexpected method %d.%d", classID, methodID)
	}

	// Skip client-properties table
	_, _ = dec.ReadTable()
	mechanism, _ := dec.ReadString()
	response, _ := dec.ReadString()

	// Special-case the legacy PLAIN mechanism backed by Authenticator.
	if mechanism == "PLAIN" {
		if len(response) < 3 {
			return ErrAuthFailed
		}
		parts := splitNull(response)
		if len(parts) < 3 {
			return ErrAuthFailed
		}
		if err := h.auth.Authenticate(parts[1], parts[2]); err != nil {
			return err
		}
		h.conn.setUsername(parts[1])
		return nil
	}

	// Delegate to registered SASL mechanism.
	m := h.sasl.Lookup(mechanism)
	if m == nil {
		return fmt.Errorf("unsupported mechanism %q", mechanism)
	}
	user, err := m.Response(h.conn.raw, []byte(response))
	if err != nil {
		return ErrAuthFailed
	}
	h.conn.setUsername(user)
	return nil
}

// sendTune sends Connection.Tune with server-chosen limits.
func (h *Handshaker) sendTune() error {
	enc := amqp091.NewEncoder()
	enc.WriteUint16(10) // Connection class
	enc.WriteUint16(30) // Tune method
	enc.WriteUint16(h.tune.channelMax)
	enc.WriteUint32(h.tune.frameMax)
	enc.WriteUint16(h.tune.heartbeat)

	return h.conn.sendMethodFrame(0, enc.Bytes())
}

// readTuneOk reads Connection.Tune-Ok and stores negotiated values.
func (h *Handshaker) readTuneOk() error {
	f, err := h.conn.readFrame()
	if err != nil {
		return err
	}
	dec := amqp091.NewDecoder(bytes.NewReader(f.Payload))
	classID, _ := dec.ReadUint16()
	methodID, _ := dec.ReadUint16()
	if classID != 10 || methodID != 31 {
		return fmt.Errorf("expected Tune-Ok, got %d.%d", classID, methodID)
	}
	chMax, _ := dec.ReadUint16()
	frmMax, _ := dec.ReadUint32()
	hb, _ := dec.ReadUint16()
	if chMax > 0 && chMax < h.tune.channelMax {
		h.tune.channelMax = chMax
	}
	if frmMax > 0 && frmMax < h.tune.frameMax {
		h.tune.frameMax = frmMax
	}
	if hb > 0 && hb < h.tune.heartbeat {
		h.tune.heartbeat = hb
	}
	return nil
}

// sendOpen sends Connection.Open with virtual host "/".
func (h *Handshaker) sendOpen() error {
	h.conn.setVHost("/")
	enc := amqp091.NewEncoder()
	enc.WriteUint16(10) // Connection class
	enc.WriteUint16(40) // Open method
	enc.WriteString("/") // virtual host
	enc.WriteString("")  // capabilities
	enc.WriteUint8(0)    // insist

	return h.conn.sendMethodFrame(0, enc.Bytes())
}

// readOpenOk reads Connection.Open-Ok.
func (h *Handshaker) readOpenOk() error {
	f, err := h.conn.readFrame()
	if err != nil {
		return err
	}
	dec := amqp091.NewDecoder(bytes.NewReader(f.Payload))
	classID, _ := dec.ReadUint16()
	methodID, _ := dec.ReadUint16()
	if classID != 10 || methodID != 41 {
		return fmt.Errorf("expected Open-Ok, got %d.%d", classID, methodID)
	}
	return nil
}

// splitNull divides a string by null bytes.
func splitNull(s string) []string {
	var parts []string
	start := 0
	for i, c := range s {
		if c == 0 {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// setDeadline sets a read deadline on the underlying connection.
func (c *Connection) setDeadline(d time.Duration) {
	if nc, ok := c.raw.(net.Conn); ok {
		nc.SetReadDeadline(time.Now().Add(d))
	}
}

// clearDeadline removes the read deadline.
func (c *Connection) clearDeadline() {
	if nc, ok := c.raw.(net.Conn); ok {
		nc.SetReadDeadline(time.Time{})
	}
}

// ErrAuthFailed is returned when credentials are invalid.
var ErrAuthFailed = fmt.Errorf("authentication failed")
