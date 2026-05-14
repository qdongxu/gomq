package server

import (
	"testing"
)

// TestPrefetchCount limits unacknowledged messages per channel.
func TestPrefetchCount(t *testing.T) {
	p := NewPrefetch()
	p.SetPrefetch(2, 0, false)

	if !p.CanDeliver(1) {
		t.Fatal("should allow first delivery")
	}
	p.RecordDelivery(1)
	if !p.CanDeliver(1) {
		t.Fatal("should allow second delivery")
	}
	p.RecordDelivery(1)
	if p.CanDeliver(1) {
		t.Fatal("should block third delivery")
	}

	p.AckDelivery(1)
	if !p.CanDeliver(1) {
		t.Fatal("should allow after ack")
	}
}

// TestGlobalPrefetch limits across all channels.
func TestGlobalPrefetch(t *testing.T) {
	p := NewPrefetch()
	p.SetPrefetch(2, 0, true)

	p.RecordDelivery(1)
	p.RecordDelivery(2)
	if p.CanDeliver(3) {
		t.Fatal("global limit should block")
	}
	p.AckDelivery(1)
	if !p.CanDeliver(3) {
		t.Fatal("should allow after one ack")
	}
}

// TestPrefetchZero means unlimited.
func TestPrefetchZero(t *testing.T) {
	p := NewPrefetch()
	p.SetPrefetch(0, 0, false)

	for i := 0; i < 10; i++ {
		p.RecordDelivery(1)
	}
	if !p.CanDeliver(1) {
		t.Fatal("zero prefetch should be unlimited")
	}
}

// TestFlowControllerPauseChannel blocks a specific channel.
func TestFlowControllerPauseChannel(t *testing.T) {
	fc := NewFlowController()
	if !fc.IsChannelActive(1) {
		t.Fatal("channel should be active by default")
	}
	fc.PauseChannel(1)
	if fc.IsChannelActive(1) {
		t.Fatal("channel should be paused")
	}
	fc.ResumeChannel(1)
	if !fc.IsChannelActive(1) {
		t.Fatal("channel should be resumed")
	}
}

// TestFlowControllerPauseConnection blocks all channels.
func TestFlowControllerPauseConnection(t *testing.T) {
	fc := NewFlowController()
	fc.PauseConnection()
	if fc.IsChannelActive(1) || fc.IsChannelActive(2) {
		t.Fatal("all channels should be paused")
	}
	fc.ResumeConnection()
	if !fc.IsChannelActive(1) || !fc.IsChannelActive(2) {
		t.Fatal("all channels should be resumed")
	}
}

// TestChannelFlowActiveWithController verifies integration.
func TestChannelFlowActiveWithController(t *testing.T) {
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	ch, _ := NewChannelManager(10).Create(1, conn)
	ch.Open()
	fc := NewFlowController()
	ch.SetFlowController(fc)

	if !ch.FlowActive() {
		t.Fatal("channel should be active")
	}
	fc.PauseChannel(1)
	if ch.FlowActive() {
		t.Fatal("channel should be paused by controller")
	}
}
