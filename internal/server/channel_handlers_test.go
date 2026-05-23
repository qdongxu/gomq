// channel_handlers_test.go tests Channel class method handlers.
package server

import (
	"testing"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

func TestHandleChannelOpen(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterChannelHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch := NewChannel(1, conn)

	// Channel starts in ChanOpening state
	if ch.State() != ChanOpening {
		t.Fatalf("expected ChanOpening, got %v", ch.State())
	}

	handler, ok := reg.Lookup(20, 10)
	if !ok {
		t.Fatal("expected Channel.Open handler registered")
	}

	err := handler(ch, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if ch.State() != ChanOpen {
		t.Fatalf("expected ChanOpen after Open, got %v", ch.State())
	}
}

func TestHandleChannelRecover(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterChannelHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch := NewChannel(1, conn)
	ch.Open()

	// Stage some deliveries
	tracker := srv.DeliveryTracker()
	msg1 := NewMessage([]byte("msg1"), Properties{})
	msg2 := NewMessage([]byte("msg2"), Properties{})
	tracker.Record(1, msg1, "q1", ch.ID())
	tracker.Record(2, msg2, "q2", ch.ID())

	if tracker.Count() != 2 {
		t.Fatalf("expected 2 tracked deliveries, got %d", tracker.Count())
	}

	handler, ok := reg.Lookup(20, 100)
	if !ok {
		t.Fatal("expected Channel.Recover handler registered")
	}

	// Encode requeue=true
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint8(0x01)
	payload := enc.Bytes()

	err := handler(ch, payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After recover, deliveries should be cleared from tracking
	if tracker.Count() != 0 {
		t.Fatalf("expected 0 tracked deliveries after recover, got %d", tracker.Count())
	}
}

func TestHandleChannelRecover_NoRequeue(t *testing.T) {
	srv := NewServer()
	reg := NewSimpleRegistry()
	RegisterChannelHandlers(reg, srv)

	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, srv)
	ch := NewChannel(1, conn)
	ch.Open()

	tracker := srv.DeliveryTracker()
	msg := NewMessage([]byte("msg"), Properties{})
	tracker.Record(1, msg, "q1", ch.ID())

	handler, ok := reg.Lookup(20, 100)
	if !ok {
		t.Fatal("expected Channel.Recover handler registered")
	}

	// Encode requeue=false
	enc := amqp091.NewEncoder()
	_ = enc.WriteUint8(0x00)
	payload := enc.Bytes()

	err := handler(ch, payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tracker.Count() != 0 {
		t.Fatalf("expected 0 tracked deliveries after recover, got %d", tracker.Count())
	}
}
