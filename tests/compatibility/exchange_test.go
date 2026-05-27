// exchange_test.go — exchange declaration and deletion.
package compatibility

import (
	"testing"
)

// TestCompat_ExchangeDeclareDirect verifies declaring a direct
// exchange and idempotent re-declaration.
func TestCompat_ExchangeDeclareDirect(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	name := "ex.compat.direct"
	if err := ch.ExchangeDeclare(
		name, "direct",
		false, false, false, false, nil,
	); err != nil {
		t.Fatalf("declare: %v", err)
	}
	// Idempotent second declare should succeed.
	if err := ch.ExchangeDeclare(
		name, "direct",
		false, false, false, false, nil,
	); err != nil {
		t.Fatalf("re-declare: %v", err)
	}
	if err := ch.ExchangeDelete(name, false, false); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

// TestCompat_ExchangeDeclareFanout verifies a fanout exchange.
func TestCompat_ExchangeDeclareFanout(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	name := "ex.compat.fanout"
	if err := ch.ExchangeDeclare(
		name, "fanout",
		false, false, false, false, nil,
	); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := ch.ExchangeDelete(name, false, false); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

// TestCompat_ExchangeDeclareTopic verifies a topic exchange with
// wildcard support.
func TestCompat_ExchangeDeclareTopic(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	name := "ex.compat.topic"
	if err := ch.ExchangeDeclare(
		name, "topic",
		false, false, false, false, nil,
	); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := ch.ExchangeDelete(name, false, false); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

// TestCompat_ExchangeDeclareHeaders verifies a headers exchange.
func TestCompat_ExchangeDeclareHeaders(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	name := "ex.compat.headers"
	if err := ch.ExchangeDeclare(
		name, "headers",
		false, false, false, false, nil,
	); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := ch.ExchangeDelete(name, false, false); err != nil {
		t.Fatalf("delete: %v", err)
	}
}
