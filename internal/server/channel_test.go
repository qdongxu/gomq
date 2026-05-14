package server

import (
	"testing"

	"github.com/qdongxu/gomq/pkg/protocol/amqp091"
)

// TestChannelOpenClose verifies channel lifecycle.
func TestChannelOpenClose(t *testing.T) {
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	mgr := NewChannelManager(10)

	ch, err := mgr.Create(1, conn)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if ch.State() != ChanOpen {
		t.Fatalf("state = %d, want Open", ch.State())
	}
	if ch.ID() != 1 {
		t.Fatalf("id = %d, want 1", ch.ID())
	}

	mgr.Remove(1)
	if mgr.Count() != 0 {
		t.Fatalf("count = %d, want 0", mgr.Count())
	}
}

// TestChannelMaxLimit rejects IDs above the limit.
func TestChannelMaxLimit(t *testing.T) {
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	mgr := NewChannelManager(5)

	_, err := mgr.Create(6, conn)
	if err == nil {
		t.Fatal("expected error for channel above limit")
	}
}

// TestChannelDuplicateID rejects duplicate channel IDs.
func TestChannelDuplicateID(t *testing.T) {
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	mgr := NewChannelManager(10)

	_, err := mgr.Create(1, conn)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err = mgr.Create(1, conn)
	if err == nil {
		t.Fatal("expected error for duplicate channel ID")
	}
}

// TestChannelFlowControl verifies flow state changes.
func TestChannelFlowControl(t *testing.T) {
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	mgr := NewChannelManager(10)

	ch, err := mgr.Create(1, conn)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !ch.FlowActive() {
		t.Fatal("flow should be active by default")
	}

	ch.SetFlow(false)
	if ch.FlowActive() {
		t.Fatal("flow should be paused")
	}

	ch.SetFlow(true)
	if !ch.FlowActive() {
		t.Fatal("flow should be active again")
	}
}

// TestDispatcherRouting routes a frame to the correct handler.
func TestDispatcherRouting(t *testing.T) {
	reg := NewSimpleRegistry()
	called := false
	reg.Register(20, 10, func(ch *Channel, payload []byte) error {
		called = true
		return nil
	})

	disp := NewDispatcher(reg)
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	mgr := NewChannelManager(10)
	ch, _ := mgr.Create(1, conn)

	// Build a method frame: class 20, method 10.
	enc := amqp091.NewEncoder()
	enc.WriteUint16(20)
	enc.WriteUint16(10)
	f := &amqp091.Frame{
		Type:    amqp091.FrameMethod,
		Channel: 1,
		Payload: enc.Bytes(),
	}

	if err := disp.Dispatch(ch, f); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if !called {
		t.Fatal("handler was not called")
	}
}

// TestDispatcherUnknownMethod returns error for unregistered methods.
func TestDispatcherUnknownMethod(t *testing.T) {
	reg := NewSimpleRegistry()
	disp := NewDispatcher(reg)
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	mgr := NewChannelManager(10)
	ch, _ := mgr.Create(1, conn)

	enc := amqp091.NewEncoder()
	enc.WriteUint16(99)
	enc.WriteUint16(99)
	f := &amqp091.Frame{
		Type:    amqp091.FrameMethod,
		Channel: 1,
		Payload: enc.Bytes(),
	}

	if err := disp.Dispatch(ch, f); err == nil {
		t.Fatal("expected error for unknown method")
	}
}

// TestChannelManagerCloseAll closes every channel.
func TestChannelManagerCloseAll(t *testing.T) {
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	mgr := NewChannelManager(10)

	for i := uint16(1); i <= 3; i++ {
		ch, err := mgr.Create(i, conn)
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		_ = ch
	}

	mgr.CloseAll()
	if mgr.Count() != 0 {
		t.Fatalf("count = %d, want 0 after CloseAll", mgr.Count())
	}
}

// TestChannelReservedZero rejects channel 0.
func TestChannelReservedZero(t *testing.T) {
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	mgr := NewChannelManager(10)

	_, err := mgr.Create(0, conn)
	if err == nil {
		t.Fatal("expected error for channel 0")
	}
}
