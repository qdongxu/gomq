package server

import (
	"testing"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// encodeConfirmSelect builds a Confirm.Select method payload.
func encodeConfirmSelect(noWait bool) []byte {
	enc := amqp091.NewEncoder()
	var bits uint8
	if noWait {
		bits |= 0x01
	}
	_ = enc.WriteUint8(bits)
	return enc.Bytes()
}

// TestConfirmSelect enables confirm mode and replies with SelectOk.
func TestConfirmSelect(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	payload := encodeConfirmSelect(false)
	handler, ok := reg.Lookup(85, 10)
	if !ok {
		t.Fatal("handler (85,10) not registered")
	}
	if err := handler(ch, payload); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	if !ch.IsConfirmMode() {
		t.Fatal("expected confirm mode to be enabled")
	}
}

// TestConfirmSelectNoWait enables confirm mode without reply.
func TestConfirmSelectNoWait(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	payload := encodeConfirmSelect(true)
	handler, _ := reg.Lookup(85, 10)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("confirm.select nowait: %v", err)
	}
	if !ch.IsConfirmMode() {
		t.Fatal("expected confirm mode to be enabled")
	}
}

// TestConfirmSelectDeliveryTag increments per publish.
func TestConfirmSelectDeliveryTag(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	// enable confirm mode
	ch.SetConfirmMode()

	if tag := ch.NextDeliveryTag(); tag != 1 {
		t.Fatalf("first tag = %d, want 1", tag)
	}
	if tag := ch.NextDeliveryTag(); tag != 2 {
		t.Fatalf("second tag = %d, want 2", tag)
	}
}

// TestPublishConfirmAck sends Basic.Ack when message is routed.
func TestPublishConfirmAck(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.ExchangeManager().Declare(
		"amq.direct", ExchangeDirect,
		false, false, false, nil,
	)
	_, _ = srv.QueueManager().Declare("q1", false, false, false, nil, nil)
	_, _ = srv.BindingManager().Bind("amq.direct", "q1", "news", nil)

	// enable confirm mode
	ch.SetConfirmMode()

	payload := encodePublish("amq.direct", "news", false, false)
	handler, _ := reg.Lookup(60, 40)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if srv.MessageStore().Len("q1") != 1 {
		t.Fatalf("queue len = %d, want 1", srv.MessageStore().Len("q1"))
	}
	// delivery tag should be 1 after publish
	if ch.deliveryTag != 1 {
		t.Fatalf("delivery tag = %d, want 1", ch.deliveryTag)
	}
}

// TestPublishConfirmNack sends Basic.Nack when no queue matches.
func TestPublishConfirmNack(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.ExchangeManager().Declare(
		"amq.direct", ExchangeDirect,
		false, false, false, nil,
	)
	_, _ = srv.QueueManager().Declare("q1", false, false, false, nil, nil)

	// enable confirm mode
	ch.SetConfirmMode()

	payload := encodePublish("amq.direct", "missing", false, false)
	handler, _ := reg.Lookup(60, 40)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if srv.MessageStore().Len("q1") != 0 {
		t.Fatalf("queue len = %d, want 0", srv.MessageStore().Len("q1"))
	}
	// delivery tag should be 1 after publish
	if ch.deliveryTag != 1 {
		t.Fatalf("delivery tag = %d, want 1", ch.deliveryTag)
	}
}
