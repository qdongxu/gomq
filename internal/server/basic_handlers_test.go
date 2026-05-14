package server

import (
	"testing"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// encodeExchangeDeclare builds an Exchange.Declare method payload.
func encodeExchangeDeclare(
	exchange, exType string,
	passive, durable, autoDelete, internal, noWait bool,
	args map[string]interface{},
) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(0) // reserved-1
	_ = enc.WriteShortString(exchange)
	_ = enc.WriteShortString(exType)
	var bits uint8
	if passive {
		bits |= 0x01
	}
	if durable {
		bits |= 0x02
	}
	if autoDelete {
		bits |= 0x04
	}
	if internal {
		bits |= 0x08
	}
	if noWait {
		bits |= 0x10
	}
	_ = enc.WriteUint8(bits)
	_ = enc.WriteTable(args)
	return enc.Bytes()
}

// encodeExchangeDelete builds an Exchange.Delete method payload.
func encodeExchangeDelete(
	exchange string,
	ifUnused, noWait bool,
) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(0) // reserved-1
	_ = enc.WriteShortString(exchange)
	var bits uint8
	if ifUnused {
		bits |= 0x01
	}
	if noWait {
		bits |= 0x02
	}
	_ = enc.WriteUint8(bits)
	return enc.Bytes()
}

// encodeQueueDeclare builds a Queue.Declare method payload.
func encodeQueueDeclare(
	queue string,
	passive, durable, exclusive, autoDelete, noWait bool,
	args map[string]interface{},
) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(0) // reserved-1
	_ = enc.WriteShortString(queue)
	var bits uint8
	if passive {
		bits |= 0x01
	}
	if durable {
		bits |= 0x02
	}
	if exclusive {
		bits |= 0x04
	}
	if autoDelete {
		bits |= 0x08
	}
	if noWait {
		bits |= 0x10
	}
	_ = enc.WriteUint8(bits)
	_ = enc.WriteTable(args)
	return enc.Bytes()
}

// encodeQueueDelete builds a Queue.Delete method payload.
func encodeQueueDelete(
	queue string,
	ifUnused, ifEmpty, noWait bool,
) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(0) // reserved-1
	_ = enc.WriteShortString(queue)
	var bits uint8
	if ifUnused {
		bits |= 0x01
	}
	if ifEmpty {
		bits |= 0x02
	}
	if noWait {
		bits |= 0x04
	}
	_ = enc.WriteUint8(bits)
	return enc.Bytes()
}

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

// TestExchangeDeclareProtocol creates an exchange via method frame.
func TestExchangeDeclareProtocol(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	payload := encodeExchangeDeclare(
		"ex1", "direct", false, false, false, false, false, nil,
	)
	handler, _ := reg.Lookup(40, 10)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if srv.ExchangeManager().Count() != 7 {
		t.Fatalf("exchanges = %d, want 7", srv.ExchangeManager().Count())
	}
}

// TestExchangeDeclarePassiveProtocol looks up an existing exchange.
func TestExchangeDeclarePassiveProtocol(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.ExchangeManager().Declare(
		"ex2", ExchangeDirect, false, false, false, nil,
	)

	payload := encodeExchangeDeclare(
		"ex2", "direct", true, false, false, false, false, nil,
	)
	handler, _ := reg.Lookup(40, 10)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("declare: %v", err)
	}
}

// TestExchangeDeleteProtocol removes an exchange via method frame.
func TestExchangeDeleteProtocol(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.ExchangeManager().Declare(
		"ex3", ExchangeDirect, false, false, false, nil,
	)

	payload := encodeExchangeDelete("ex3", false, false)
	handler, _ := reg.Lookup(40, 20)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if srv.ExchangeManager().Count() != 6 {
		t.Fatalf("exchanges = %d, want 6", srv.ExchangeManager().Count())
	}
}

// TestQueueDeclareProtocol creates a queue via method frame.
func TestQueueDeclareProtocol(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	payload := encodeQueueDeclare("q2", false, false, false, false, false, nil)
	handler, _ := reg.Lookup(50, 10)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if srv.QueueManager().Count() != 1 {
		t.Fatalf("queues = %d, want 1", srv.QueueManager().Count())
	}
}

// TestQueueDeclarePassiveProtocol looks up an existing queue.
func TestQueueDeclarePassiveProtocol(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.QueueManager().Declare("q3", false, false, false, nil, nil)

	payload := encodeQueueDeclare("q3", true, false, false, false, false, nil)
	handler, _ := reg.Lookup(50, 10)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("declare: %v", err)
	}
}

// TestQueueDeleteProtocol removes a queue via method frame.
func TestQueueDeleteProtocol(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.QueueManager().Declare("q4", false, false, false, nil, nil)

	payload := encodeQueueDelete("q4", false, false, false)
	handler, _ := reg.Lookup(50, 40)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if srv.QueueManager().Count() != 0 {
		t.Fatalf("queues = %d, want 0", srv.QueueManager().Count())
	}
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

// encodeQos builds a Basic.Qos method payload.
func encodeQos(prefetchSize uint32, prefetchCount uint16, global bool) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint32(prefetchSize)
	_ = enc.WriteUint16(prefetchCount)
	var bits uint8
	if global {
		bits |= 0x01
	}
	_ = enc.WriteUint8(bits)
	return enc.Bytes()
}

// encodeConnectionClose builds a Connection.Close method payload.
func encodeConnectionClose(
	replyCode uint16,
	replyText string,
	classID, methodID uint16,
) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(replyCode)
	_ = enc.WriteShortString(replyText)
	_ = enc.WriteUint16(classID)
	_ = enc.WriteUint16(methodID)
	return enc.Bytes()
}

// encodeChannelClose builds a Channel.Close method payload.
func encodeChannelClose(
	replyCode uint16,
	replyText string,
	classID, methodID uint16,
) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(replyCode)
	_ = enc.WriteShortString(replyText)
	_ = enc.WriteUint16(classID)
	_ = enc.WriteUint16(methodID)
	return enc.Bytes()
}

// TestConnectionClose sends Connection.Close and verifies
// the connection transitions to Closing state.
func TestConnectionClose(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	payload := encodeConnectionClose(200, "normal", 0, 0)
	handler, _ := reg.Lookup(10, 50)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("close: %v", err)
	}
	if conn.State() != StateClosed {
		t.Fatalf("state = %d, want StateClosed", conn.State())
	}
}

// TestChannelClose sends Channel.Close and verifies
// the channel transitions to ChanClosed state.
func TestChannelClose(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	payload := encodeChannelClose(200, "normal", 0, 0)
	handler, _ := reg.Lookup(20, 40)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("close: %v", err)
	}
	if ch.State() != ChanClosed {
		t.Fatalf("state = %d, want ChanClosed", ch.State())
	}
}

// encodeRecover builds a Basic.Recover method payload.
func encodeRecover(requeue bool) []byte {
	enc := amqp091.NewEncoder()
	var bits uint8
	if requeue {
		bits |= 0x01
	}
	_ = enc.WriteUint8(bits)
	return enc.Bytes()
}

// TestBasicRecoverRequeue returns all unacked messages to their queue.
func TestBasicRecoverRequeue(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	srv.MessageStore().Enqueue("q1", NewMessage([]byte("a"), Properties{}))
	srv.MessageStore().Enqueue("q1", NewMessage([]byte("b"), Properties{}))

	pc := NewPullConsumer(srv.MessageStore(), srv.DeliveryTracker())
	_, _ = pc.Get("q1", false, 1)
	_, _ = pc.Get("q1", false, 1)

	if srv.DeliveryTracker().Count() != 2 {
		t.Fatalf("tracker = %d, want 2", srv.DeliveryTracker().Count())
	}

	payload := encodeRecover(true)
	handler, _ := reg.Lookup(60, 110)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if srv.DeliveryTracker().Count() != 0 {
		t.Fatalf("tracker = %d, want 0", srv.DeliveryTracker().Count())
	}
	if srv.MessageStore().Len("q1") != 2 {
		t.Fatalf("queue len = %d, want 2", srv.MessageStore().Len("q1"))
	}
}

// TestBasicRecoverNoRequeue discards all unacked messages.
func TestBasicRecoverNoRequeue(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	srv.MessageStore().Enqueue("q1", NewMessage([]byte("c"), Properties{}))

	pc := NewPullConsumer(srv.MessageStore(), srv.DeliveryTracker())
	_, _ = pc.Get("q1", false, 1)

	if srv.DeliveryTracker().Count() != 1 {
		t.Fatalf("tracker = %d, want 1", srv.DeliveryTracker().Count())
	}

	payload := encodeRecover(false)
	handler, _ := reg.Lookup(60, 110)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if srv.DeliveryTracker().Count() != 0 {
		t.Fatalf("tracker = %d, want 0", srv.DeliveryTracker().Count())
	}
	if srv.MessageStore().Len("q1") != 0 {
		t.Fatalf("queue len = %d, want 0", srv.MessageStore().Len("q1"))
	}
}

// encodeQueueBind builds a Queue.Bind method payload.
func encodeQueueBind(
	queue, exchange, routingKey string,
	noWait bool,
	args map[string]interface{},
) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(0) // reserved-1
	_ = enc.WriteShortString(queue)
	_ = enc.WriteShortString(exchange)
	_ = enc.WriteShortString(routingKey)
	var bits uint8
	if noWait {
		bits |= 0x01
	}
	_ = enc.WriteUint8(bits)
	_ = enc.WriteTable(args)
	return enc.Bytes()
}

// encodeQueueUnbind builds a Queue.Unbind method payload.
func encodeQueueUnbind(
	queue, exchange, routingKey string,
	args map[string]interface{},
) []byte {
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint16(0) // reserved-1
	_ = enc.WriteShortString(queue)
	_ = enc.WriteShortString(exchange)
	_ = enc.WriteShortString(routingKey)
	_ = enc.WriteTable(args)
	return enc.Bytes()
}

// TestQueueBindProtocol creates a binding via method frame.
func TestQueueBindProtocol(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	payload := encodeQueueBind("q1", "", "key1", false, nil)
	handler, _ := reg.Lookup(50, 20)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if len(srv.BindingManager().GetBindings("")) != 1 {
		t.Fatalf("bindings = %d, want 1", len(srv.BindingManager().GetBindings("")))
	}
}

// TestQueueUnbindProtocol removes a binding via method frame.
func TestQueueUnbindProtocol(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	_, _ = srv.BindingManager().Bind("", "q1", "key1", nil)

	payload := encodeQueueUnbind("q1", "", "key1", nil)
	handler, _ := reg.Lookup(50, 50)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if len(srv.BindingManager().GetBindings("")) != 0 {
		t.Fatalf("bindings = %d, want 0", len(srv.BindingManager().GetBindings("")))
	}
}

// TestBasicQos sets a per-channel prefetch limit via protocol frame.
func TestBasicQos(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	payload := encodeQos(0, 3, false)
	handler, _ := reg.Lookup(60, 10)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("qos: %v", err)
	}

	// verify the limit is active
	p := srv.Prefetch()
	p.RecordDelivery(1)
	p.RecordDelivery(1)
	if !p.CanDeliver(1) {
		t.Fatal("should allow up to prefetch count")
	}
	p.RecordDelivery(1)
	if p.CanDeliver(1) {
		t.Fatal("should block after reaching prefetch count")
	}
}

// TestBasicQosGlobal sets a connection-level prefetch limit.
func TestBasicQosGlobal(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterBasicHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()

	payload := encodeQos(0, 2, true)
	handler, _ := reg.Lookup(60, 10)
	if err := handler(ch, payload); err != nil {
		t.Fatalf("qos global: %v", err)
	}

	// global limit applies across channels
	p := srv.Prefetch()
	p.RecordDelivery(1)
	p.RecordDelivery(2)
	if p.CanDeliver(3) {
		t.Fatal("global limit should block new delivery")
	}
	p.AckDelivery(1)
	if !p.CanDeliver(3) {
		t.Fatal("global limit should allow after ack")
	}
}
