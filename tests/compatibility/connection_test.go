// connection_test.go — connection establishment and authentication.
package compatibility

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestCompat_ConnectionOpen verifies that an amqp091-go client can
// establish a TCP connection, perform the AMQP handshake, and open
// a connection.
func TestCompat_ConnectionOpen(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	defer stopServer(t, srv)

	conn := dial(t, addr)
	if conn.IsClosed() {
		t.Fatal("connection closed immediately after dial")
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestCompat_ConnectionAuth verifies SASL PLAIN authentication with
// the default guest/guest credentials.
func TestCompat_ConnectionAuth(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	defer stopServer(t, srv)

	conn := dial(t, addr)
	defer conn.Close()

	ch := openChannel(t, conn)
	if err := ch.Close(); err != nil {
		t.Fatalf("close channel: %v", err)
	}
}

// TestCompat_ConnectionMultipleChannels verifies that a single
// connection supports multiple concurrent channels.
func TestCompat_ConnectionMultipleChannels(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	defer stopServer(t, srv)

	conn := dial(t, addr)
	defer conn.Close()

	const n = 5
	channels := make([]*amqp.Channel, n)
	for i := 0; i < n; i++ {
		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("channel %d: %v", i, err)
		}
		channels[i] = ch
	}
	for i, ch := range channels {
		if err := ch.Close(); err != nil {
			t.Fatalf("close channel %d: %v", i, err)
		}
	}
}
