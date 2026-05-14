package server

import (
	"testing"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

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
	// no response expected; channel has no frame capture here
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
