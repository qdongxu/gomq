package server

import (
	"testing"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// encodePublish builds a Basic.Publish method payload.
func encodePublish(
	exchange, routingKey string,
	mandatory, immediate bool,
) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(0) // reserved-1
	_ = enc.WriteShortString(exchange)
	_ = enc.WriteShortString(routingKey)
	var bits uint8
	if mandatory {
		bits |= 0x01
	}
	if immediate {
		bits |= 0x02
	}
	_ = enc.WriteUint8(bits)
	return enc.Bytes()
}

// encodeGet builds a Basic.Get method payload.
func encodeGet(queue string, noAck bool) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(0) // reserved-1
	_ = enc.WriteShortString(queue)
	var bits uint8
	if noAck {
		bits |= 0x01
	}
	_ = enc.WriteUint8(bits)
	return enc.Bytes()
}

// encodeAck builds a Basic.Ack method payload.
func encodeAck(tag uint64) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint64(tag)
	return enc.Bytes()
}

// encodeNack builds a Basic.Nack method payload.
func encodeNack(tag uint64, requeue bool) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint64(tag)
	var bits uint8
	if requeue {
		bits |= 0x01
	}
	_ = enc.WriteUint8(bits)
	return enc.Bytes()
}

// encodeReject builds a Basic.Reject method payload.
func encodeReject(tag uint64, requeue bool) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint64(tag)
	var bits uint8
	if requeue {
		bits |= 0x01
	}
	_ = enc.WriteUint8(bits)
	return enc.Bytes()
}

// encodeConsume builds a Basic.Consume method payload.
func encodeConsume(
	queue, tag string,
	noLocal, noAck, exclusive, noWait bool,
	args map[string]interface{},
) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(0) // reserved-1
	_ = enc.WriteShortString(queue)
	_ = enc.WriteShortString(tag)
	var bits uint8
	if noLocal {
		bits |= 0x01
	}
	if noAck {
		bits |= 0x02
	}
	if exclusive {
		bits |= 0x04
	}
	if noWait {
		bits |= 0x08
	}
	_ = enc.WriteUint8(bits)
	_ = enc.WriteTable(args)
	return enc.Bytes()
}

// encodeCancel builds a Basic.Cancel method payload.
func encodeCancel(tag string, noWait bool) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteShortString(tag)
	var bits uint8
	if noWait {
		bits |= 0x01
	}
	_ = enc.WriteUint8(bits)
	return enc.Bytes()
}

// TestBasicPublish routes a message via method frame.
func TestBasicPublish(t *testing.T) {
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

	payload := encodePublish("amq.direct", "news", false, false)
	handler, _ := reg.Lookup(60, 40)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if srv.MessageStore().Len("q1") != 1 {
		t.Fatalf("queue len = %d, want 1", srv.MessageStore().Len("q1"))
	}
}

// TestBasicPublishNoRoute stores nothing when no queue matches.
func TestBasicPublishNoRoute(t *testing.T) {
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

	payload := encodePublish("amq.direct", "missing", false, false)
	handler, _ := reg.Lookup(60, 40)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if srv.MessageStore().Len("q1") != 0 {
		t.Fatalf("queue len = %d, want 0", srv.MessageStore().Len("q1"))
	}
}

// TestBasicGetEmpty returns empty when queue has no messages.
func TestBasicGetEmpty(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	payload := encodeGet("q1", true)
	handler, _ := reg.Lookup(60, 70)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("get: %v", err)
	}
}

// TestBasicAck confirms a tracked delivery.
func TestBasicAck(t *testing.T) {
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

	// publish a message and get it to create a tracked delivery
	srv.MessageStore().Enqueue("q1", NewMessage([]byte("ack"), Properties{}))
	pc := NewPullConsumer(srv.MessageStore(), srv.DeliveryTracker())
	msg, _ := pc.Get("q1", false, 1)

	if srv.DeliveryTracker().Count() != 1 {
		t.Fatalf("tracker = %d, want 1", srv.DeliveryTracker().Count())
	}

	payload := encodeAck(msg.DeliveryTag())
	handler, _ := reg.Lookup(60, 80)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if srv.DeliveryTracker().Count() != 0 {
		t.Fatalf("tracker = %d, want 0", srv.DeliveryTracker().Count())
	}
}

// TestBasicNack rejects and requeues a delivery.
func TestBasicNack(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	srv.MessageStore().Enqueue("q1", NewMessage([]byte("nack"), Properties{}))
	pc := NewPullConsumer(srv.MessageStore(), srv.DeliveryTracker())
	msg, _ := pc.Get("q1", false, 1)

	if srv.MessageStore().Len("q1") != 0 {
		t.Fatalf("queue len = %d, want 0", srv.MessageStore().Len("q1"))
	}

	payload := encodeNack(msg.DeliveryTag(), true)
	handler, _ := reg.Lookup(60, 120)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("nack: %v", err)
	}
	if srv.MessageStore().Len("q1") != 1 {
		t.Fatalf("queue len = %d, want 1", srv.MessageStore().Len("q1"))
	}
}

// TestBasicReject rejects a delivery without requeue.
func TestBasicReject(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	srv.MessageStore().Enqueue("q1", NewMessage([]byte("reject"), Properties{}))
	pc := NewPullConsumer(srv.MessageStore(), srv.DeliveryTracker())
	msg, _ := pc.Get("q1", false, 1)

	if srv.DeliveryTracker().Count() != 1 {
		t.Fatalf("tracker = %d, want 1", srv.DeliveryTracker().Count())
	}

	payload := encodeReject(msg.DeliveryTag(), false)
	handler, _ := reg.Lookup(60, 90)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("reject: %v", err)
	}
	if srv.DeliveryTracker().Count() != 0 {
		t.Fatalf("tracker = %d, want 0", srv.DeliveryTracker().Count())
	}
}

// TestBasicConsume subscribes a consumer via method frame.
func TestBasicConsume(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	srv.QueueManager().Declare("q1", false, false, false, nil, nil)

	payload := encodeConsume("q1", "c1", false, false, false, false, nil)
	handler, _ := reg.Lookup(60, 20)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if srv.ConsumerManager().Count() != 1 {
		t.Fatalf("consumers = %d, want 1", srv.ConsumerManager().Count())
	}
}

// TestBasicConsumeNoWait does not send response.
func TestBasicConsumeNoWait(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	srv.QueueManager().Declare("q1", false, false, false, nil, nil)

	payload := encodeConsume("q1", "c2", false, false, false, true, nil)
	handler, _ := reg.Lookup(60, 20)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("consume: %v", err)
	}
}

// TestBasicCancel unsubscribes a consumer via method frame.
func TestBasicCancel(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	srv.QueueManager().Declare("q1", false, false, false, nil, nil)
	_, _ = srv.ConsumerManager().Subscribe("c3", "q1", ch, false, false, nil)

	payload := encodeCancel("c3", false)
	handler, _ := reg.Lookup(60, 30)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if srv.ConsumerManager().Count() != 0 {
		t.Fatalf("consumers = %d, want 0", srv.ConsumerManager().Count())
	}
}

// TestBasicCancelNoWait does not send response.
func TestBasicCancelNoWait(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	srv.QueueManager().Declare("q1", false, false, false, nil, nil)
	_, _ = srv.ConsumerManager().Subscribe("c4", "q1", ch, false, false, nil)

	payload := encodeCancel("c4", true)
	handler, _ := reg.Lookup(60, 30)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("cancel: %v", err)
	}
}
