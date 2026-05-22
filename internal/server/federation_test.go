// federation_test.go tests the FederationConfig type.
package server

import "testing"

func TestNewFederationConfig(t *testing.T) {
	cfg := NewFederationConfig("fed-1")
	if cfg.Name != "fed-1" {
		t.Fatalf("expected name fed-1, got %q", cfg.Name)
	}
	if cfg.ReconnectDelay != 5 {
		t.Fatalf("expected reconnect delay 5, got %d", cfg.ReconnectDelay)
	}
	if cfg.PrefetchCount != 10 {
		t.Fatalf("expected prefetch 10, got %d", cfg.PrefetchCount)
	}
	if cfg.AckMode != AckOnConfirm {
		t.Fatalf("expected ack mode on-confirm, got %q", cfg.AckMode)
	}
}

func TestFederationConfig_Fields(t *testing.T) {
	cfg := NewFederationConfig("fed-2")
	cfg.Upstreams = []string{"amqp://node-a:5672", "amqp://node-b:5672"}
	cfg.Exchange = "events"
	cfg.Queue = "events.q"
	cfg.RoutingKey = "user.#"
	cfg.AckMode = AckNoAck

	if len(cfg.Upstreams) != 2 {
		t.Fatalf("expected 2 upstreams, got %d", len(cfg.Upstreams))
	}
	if cfg.AckMode != AckNoAck {
		t.Fatalf("expected ack mode no-ack, got %q", cfg.AckMode)
	}
}
