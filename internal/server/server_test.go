package server

import (
	"testing"
)

// TestServerNew creates a server with all managers.
func TestServerNew(t *testing.T) {
	s := NewServer()
	if s == nil {
		t.Fatal("expected server")
	}
	if s.ExchangeManager() == nil {
		t.Fatal("expected exchange manager")
	}
	if s.QueueManager() == nil {
		t.Fatal("expected queue manager")
	}
	if s.MessageStore() == nil {
		t.Fatal("expected message store")
	}
}

// TestServerListen starts a TCP listener.
func TestServerListen(t *testing.T) {
	s := NewServer()
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("listen: %v", err)
	}
	_ = s.Shutdown()
}
