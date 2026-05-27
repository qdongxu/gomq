// queue_test.go — queue declaration, binding, unbinding and deletion.
package compatibility

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestCompat_QueueDeclare verifies queue declaration and idempotent
// re-declaration.
func TestCompat_QueueDeclare(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	name := "q.compat.declare"
	q, err := ch.QueueDeclare(
		name, false, false, false, false, nil,
	)
	if err != nil {
		t.Fatalf("declare: %v", err)
	}
	if q.Name != name {
		t.Fatalf("name mismatch: got %q, want %q", q.Name, name)
	}

	// Idempotent re-declare.
	_, err = ch.QueueDeclare(name, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("re-declare: %v", err)
	}

	_, err = ch.QueueDelete(name, false, false, false)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
}

// TestCompat_QueueBindUnbind verifies queue binding and unbinding.
func TestCompat_QueueBindUnbind(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	ex := "ex.compat.bind"
	q := "q.compat.bind"
	rk := "routing.key"

	if err := ch.ExchangeDeclare(ex, "direct",
		false, false, false, false, nil); err != nil {
		t.Fatalf("exchange declare: %v", err)
	}
	if _, err := ch.QueueDeclare(q, false, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	if err := ch.QueueBind(q, rk, ex, false, nil); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := ch.QueueUnbind(q, rk, ex, nil); err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if err := ch.ExchangeDelete(ex, false, false); err != nil {
		t.Fatalf("exchange delete: %v", err)
	}
	if _, err := ch.QueueDelete(q, false, false, false); err != nil {
		t.Fatalf("queue delete: %v", err)
	}
}

// TestCompat_QueuePurge verifies that Queue.Purge removes all
// messages and returns the count.
func TestCompat_QueuePurge(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	ex := "ex.compat.purge"
	q := "q.compat.purge"
	rk := "rk"

	if err := ch.ExchangeDeclare(ex, "direct",
		false, false, false, false, nil); err != nil {
		t.Fatalf("exchange declare: %v", err)
	}
	if _, err := ch.QueueDeclare(q, false, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	if err := ch.QueueBind(q, rk, ex, false, nil); err != nil {
		t.Fatalf("bind: %v", err)
	}

	// Publish a few messages.
	for i := 0; i < 5; i++ {
		if err := ch.Publish(ex, rk, false, false,
			amqp.Publishing{Body: []byte("msg")}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	count, err := ch.QueuePurge(q, false)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if count != 5 {
		t.Fatalf("purge count: got %d, want 5", count)
	}

	if _, err := ch.QueueDelete(q, false, false, false); err != nil {
		t.Fatalf("queue delete: %v", err)
	}
	if err := ch.ExchangeDelete(ex, false, false); err != nil {
		t.Fatalf("exchange delete: %v", err)
	}
}
