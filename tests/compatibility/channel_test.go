// channel_test.go — Tx, QoS and Publisher Confirm.
package compatibility

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestCompat_ChannelTxCommit verifies a successful transaction.
func TestCompat_ChannelTxCommit(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	if err := ch.Tx(); err != nil {
		t.Fatalf("tx.select: %v", err)
	}

	ex := "ex.compat.tx"
	q := "q.compat.tx"
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

	if err := ch.Publish(ex, rk, false, false,
		amqp.Publishing{Body: []byte("tx-msg")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if err := ch.TxCommit(); err != nil {
		t.Fatalf("tx.commit: %v", err)
	}

	// Verify the message is visible after commit.
	msgs, err := ch.Consume(q, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	select {
	case msg := <-msgs:
		if string(msg.Body) != "tx-msg" {
			t.Fatalf("body mismatch: got %q", msg.Body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message after commit")
	}

	if _, err := ch.QueueDelete(q, false, false, false); err != nil {
		t.Fatalf("queue delete: %v", err)
	}
	if err := ch.ExchangeDelete(ex, false, false); err != nil {
		t.Fatalf("exchange delete: %v", err)
	}
}

// TestCompat_ChannelTxRollback verifies that a rolled-back transaction
// makes messages invisible.
func TestCompat_ChannelTxRollback(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	if err := ch.Tx(); err != nil {
		t.Fatalf("tx.select: %v", err)
	}

	ex := "ex.compat.txrb"
	q := "q.compat.txrb"
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

	if err := ch.Publish(ex, rk, false, false,
		amqp.Publishing{Body: []byte("rollback-msg")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if err := ch.TxRollback(); err != nil {
		t.Fatalf("tx.rollback: %v", err)
	}

	// After rollback the queue should be empty.
	_, ok, err := ch.Get(q, false)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatal("expected empty queue after rollback")
	}

	if _, err := ch.QueueDelete(q, false, false, false); err != nil {
		t.Fatalf("queue delete: %v", err)
	}
	if err := ch.ExchangeDelete(ex, false, false); err != nil {
		t.Fatalf("exchange delete: %v", err)
	}
}

// TestCompat_ChannelQosPrefetchCount verifies QoS prefetch count.
func TestCompat_ChannelQosPrefetchCount(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	if err := ch.Qos(3, 0, false); err != nil {
		t.Fatalf("qos: %v", err)
	}
	// No direct observable side-effect to assert here; the test
	// succeeds if the server accepted the QoS request without error.
}

// TestCompat_ChannelQosPrefetchSize verifies QoS prefetch size.
func TestCompat_ChannelQosPrefetchSize(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	if err := ch.Qos(0, 4096, false); err != nil {
		t.Fatalf("qos: %v", err)
	}
}

// TestCompat_ChannelQosGlobal verifies global QoS flag.
func TestCompat_ChannelQosGlobal(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	if err := ch.Qos(1, 0, true); err != nil {
		t.Fatalf("qos global: %v", err)
	}
}

// TestCompat_ChannelConfirm verifies Publisher Confirms.
func TestCompat_ChannelConfirm(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	ex := "ex.compat.confirm"
	q := "q.compat.confirm"
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

	if err := ch.Publish(ex, rk, false, false,
		amqp.Publishing{Body: []byte("confirm-msg")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case c := <-confirms:
		if !c.Ack {
			t.Fatal("expected ack confirm")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for confirm")
	}

	if _, err := ch.QueueDelete(q, false, false, false); err != nil {
		t.Fatalf("queue delete: %v", err)
	}
	if err := ch.ExchangeDelete(ex, false, false); err != nil {
		t.Fatalf("exchange delete: %v", err)
	}
}
