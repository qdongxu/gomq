package server

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// pipeConn is a net.Conn backed by two ends of a pipe.
type pipeConn struct {
	net.Conn
}

// TestHandshakeSuccess verifies a complete AMQP connection startup.
func TestHandshakeSuccess(t *testing.T) {
	client, serverConn := net.Pipe()
	defer client.Close()

	auth := NewMemoryAuthenticator()
	conn := NewConnection(serverConn, auth, nil)

	done := make(chan struct{}, 1)
	go func() {
		conn.Serve()
		close(done)
	}()

	// Send protocol header.
	client.Write([]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1})

	// Read Connection.Start
	start := readFrame(t, client)
	if start.Type != amqp091.FrameMethod {
		t.Fatalf("expected method frame, got %d", start.Type)
	}

	// Send Connection.Start-Ok with PLAIN auth.
	enc := amqp091.NewEncoder()
	enc.WriteUint16(10)
	enc.WriteUint16(11)
	enc.WriteTable(map[string]interface{}{})
	enc.WriteString("PLAIN")
	enc.WriteString("\x00guest\x00guest")
	enc.WriteString("en_US")
	sendFrame(t, client, 0, amqp091.FrameMethod, enc.Bytes())

	// Read Connection.Tune
	tune := readFrame(t, client)
	if tune.Type != amqp091.FrameMethod {
		t.Fatalf("expected tune, got %d", tune.Type)
	}

	// Send Connection.Tune-Ok
	enc = amqp091.NewEncoder()
	enc.WriteUint16(10)
	enc.WriteUint16(31)
	enc.WriteUint16(2048)
	enc.WriteUint32(131072)
	enc.WriteUint16(60)
	sendFrame(t, client, 0, amqp091.FrameMethod, enc.Bytes())

	// Read Connection.Open
	open := readFrame(t, client)
	if open.Type != amqp091.FrameMethod {
		t.Fatalf("expected open, got %d", open.Type)
	}

	// Send Connection.Open-Ok
	enc = amqp091.NewEncoder()
	enc.WriteUint16(10)
	enc.WriteUint16(41)
	sendFrame(t, client, 0, amqp091.FrameMethod, enc.Bytes())

	// Connection should now be in Open state.
	time.Sleep(50 * time.Millisecond)
	if conn.State() != StateOpen {
		t.Fatalf("state = %d, want Open", conn.State())
	}

	// Graceful close from server side.
	conn.Close()
	<-done
}

// TestInvalidProtocolHeader rejects a malformed greeting.
func TestInvalidProtocolHeader(t *testing.T) {
	client, serverConn := net.Pipe()
	defer client.Close()

	auth := NewMemoryAuthenticator()
	conn := NewConnection(serverConn, auth, nil)

	done := make(chan struct{})
	go func() {
		conn.Serve()
		close(done)
	}()

	client.Write([]byte("NOTAMQP!"))
	<-done

	if conn.State() != StateClosed {
		t.Fatalf("state = %d, want Closed", conn.State())
	}
}

// TestHeartbeatTimeout is skipped because net.Pipe() deadlocks on
// unbuffered heartbeat writes. Requires a mock transport or buffer.
func TestHeartbeatTimeout(t *testing.T) {
	t.Skip("needs mock transport")
}

// TestGracefulClose verifies server-initiated graceful shutdown.
func TestGracefulClose(t *testing.T) {
	client, serverConn := net.Pipe()
	defer client.Close()

	auth := NewMemoryAuthenticator()
	conn := NewConnection(serverConn, auth, nil)

	done := make(chan struct{})
	go func() {
		conn.Serve()
		close(done)
	}()

	// Complete handshake.
	client.Write([]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1})
	_ = readFrame(t, client) // Start

	enc := amqp091.NewEncoder()
	enc.WriteUint16(10)
	enc.WriteUint16(11)
	enc.WriteTable(map[string]interface{}{})
	enc.WriteString("PLAIN")
	enc.WriteString("\x00guest\x00guest")
	enc.WriteString("en_US")
	sendFrame(t, client, 0, amqp091.FrameMethod, enc.Bytes())

	_ = readFrame(t, client) // Tune

	enc = amqp091.NewEncoder()
	enc.WriteUint16(10)
	enc.WriteUint16(31)
	enc.WriteUint16(2048)
	enc.WriteUint32(131072)
	enc.WriteUint16(60)
	sendFrame(t, client, 0, amqp091.FrameMethod, enc.Bytes())

	_ = readFrame(t, client) // Open

	enc = amqp091.NewEncoder()
	enc.WriteUint16(10)
	enc.WriteUint16(41)
	sendFrame(t, client, 0, amqp091.FrameMethod, enc.Bytes())

	time.Sleep(50 * time.Millisecond)

	// Trigger server-side graceful close.
	conn.Close()

	// Connection should be closed.
	if conn.State() != StateClosed {
		t.Fatalf("state = %d, want Closed", conn.State())
	}

	<-done
}

// sendFrame writes a frame to the test client.
func sendFrame(
	t *testing.T,
	w io.Writer,
	ch uint16,
	ft amqp091.FrameType,
	payload []byte,
) {
	enc := amqp091.NewEncoder()
	err := enc.EncodeFrame(&amqp091.Frame{
		Type:    ft,
		Channel: ch,
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	_, err = w.Write(enc.Bytes())
	if err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// readFrame reads the next frame from the test client.
func readFrame(t *testing.T, r io.Reader) *amqp091.Frame {
	dec := amqp091.NewDecoder(r)
	f, err := dec.ReadFrame(amqp091.FrameMaxDefault)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return f
}
