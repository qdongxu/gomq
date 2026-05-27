// basic_test.go — Basic.Publish, Consume, Get, Ack, Nack, Reject.
package compatibility

import (
	"bytes"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TestCompat_BasicPublishConsume verifies a round-trip publish →
// consume → ack with manual acknowledgement.
func TestCompat_BasicPublishConsume(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	ex := "ex.compat.pub"
	q := "q.compat.pub"
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

	body := []byte("hello-compat")
	if err := ch.Publish(ex, rk, false, false,
		amqp.Publishing{Body: body}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs, err := ch.Consume(q, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if !bytes.Equal(msg.Body, body) {
			t.Fatalf("body mismatch: got %q, want %q", msg.Body, body)
		}
		if err := msg.Ack(false); err != nil {
			t.Fatalf("ack: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	if _, err := ch.QueueDelete(q, false, false, false); err != nil {
		t.Fatalf("queue delete: %v", err)
	}
	if err := ch.ExchangeDelete(ex, false, false); err != nil {
		t.Fatalf("exchange delete: %v", err)
	}
}

// TestCompat_BasicConsumeAutoAck verifies consume with auto-ack.
func TestCompat_BasicConsumeAutoAck(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	ex := "ex.compat.auto"
	q := "q.compat.auto"
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
		amqp.Publishing{Body: []byte("auto-ack")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs, err := ch.Consume(q, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != "auto-ack" {
			t.Fatalf("body mismatch: got %q", msg.Body)
		}
		// auto-ack means no explicit Ack required.
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	if _, err := ch.QueueDelete(q, false, false, false); err != nil {
		t.Fatalf("queue delete: %v", err)
	}
	if err := ch.ExchangeDelete(ex, false, false); err != nil {
		t.Fatalf("exchange delete: %v", err)
	}
}

// TestCompat_BasicGet verifies Basic.Get (poll-style consume).
func TestCompat_BasicGet(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	ex := "ex.compat.get"
	q := "q.compat.get"
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

	// Queue is empty — Get should return false.
	msg, ok, err := ch.Get(q, false)
	if err != nil {
		t.Fatalf("get empty: %v", err)
	}
	if ok {
		t.Fatal("expected no message from empty queue")
	}

	// Publish then Get.
	if err := ch.Publish(ex, rk, false, false,
		amqp.Publishing{Body: []byte("get-me")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, ok, err = ch.Get(q, false)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("expected message")
	}
	if string(msg.Body) != "get-me" {
		t.Fatalf("body mismatch: got %q", msg.Body)
	}
	if err := msg.Ack(false); err != nil {
		t.Fatalf("ack: %v", err)
	}

	if _, err := ch.QueueDelete(q, false, false, false); err != nil {
		t.Fatalf("queue delete: %v", err)
	}
	if err := ch.ExchangeDelete(ex, false, false); err != nil {
		t.Fatalf("exchange delete: %v", err)
	}
}

// TestCompat_BasicNackRequeue verifies Nack with requeue=true.
func TestCompat_BasicNackRequeue(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	ex := "ex.compat.nack"
	q := "q.compat.nack"
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
		amqp.Publishing{Body: []byte("nack-me")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs, err := ch.Consume(q, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if err := msg.Nack(false, true); err != nil {
			t.Fatalf("nack: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// Message should be requeued and delivered again.
	select {
	case msg := <-msgs:
		if string(msg.Body) != "nack-me" {
			t.Fatalf("body mismatch: got %q", msg.Body)
		}
		if err := msg.Ack(false); err != nil {
			t.Fatalf("ack: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for requeued message")
	}

	if _, err := ch.QueueDelete(q, false, false, false); err != nil {
		t.Fatalf("queue delete: %v", err)
	}
	if err := ch.ExchangeDelete(ex, false, false); err != nil {
		t.Fatalf("exchange delete: %v", err)
	}
}

// TestCompat_BasicReject verifies Basic.Reject with requeue=false.
func TestCompat_BasicReject(t *testing.T) {
	srv := compatServer()
	addr := startServer(t, srv)
	conn := dial(t, addr)
	defer cleanup(t, conn, srv)
	ch := openChannel(t, conn)

	ex := "ex.compat.reject"
	q := "q.compat.reject"
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
		amqp.Publishing{Body: []byte("reject-me")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs, err := ch.Consume(q, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if err := msg.Reject(false); err != nil {
			t.Fatalf("reject: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	// No more messages should be available.
	_, ok, err := ch.Get(q, false)
	if err != nil {
		t.Fatalf("get after reject: %v", err)
	}
	if ok {
		t.Fatal("expected queue to be empty after reject")
	}

	if _, err := ch.QueueDelete(q, false, false, false); err != nil {
		t.Fatalf("queue delete: %v", err)
	}
	if err := ch.ExchangeDelete(ex, false, false); err != nil {
		t.Fatalf("exchange delete: %v", err)
	}
}
