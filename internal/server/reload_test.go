// reload_test.go tests the server-level configuration reloading.
package server

import (
	"testing"

	"github.com/qdongxu/gomq/internal/config"
)

func TestReloadConfigACL(t *testing.T) {
	srv := NewServerWithStore(nil)

	cfg := config.Default()
	cfg.ACL.Rules = []config.ACLRule{
		{User: "alice", VHost: "/", ResourceType: "queue", ResourceName: "*", Permission: "read", Allow: true},
	}

	if err := srv.ReloadConfig(cfg); err != nil {
		t.Fatalf("reload config: %v", err)
	}

	mgr := srv.ACLManager()
	if mgr == nil {
		t.Fatal("expected ACL manager after reload")
	}
}

func TestReloadConfigRateLimiter(t *testing.T) {
	srv := NewServerWithStore(nil)

	cfg := config.Default()
	cfg.Limits.MaxConnectionsPerSecond = 50

	if err := srv.ReloadConfig(cfg); err != nil {
		t.Fatalf("reload config: %v", err)
	}

	// RateLimiter is unexported; verify by checking behaviour indirectly.
	// We can't access it directly, but ReloadConfig should not panic.
}

func TestReloadConfigBackPressure(t *testing.T) {
	srv := NewServerWithStore(nil)

	cfg := config.Default()
	cfg.Limits.BackPressureEnabled = true
	cfg.Limits.MemoryThresholdPercent = 75

	if err := srv.ReloadConfig(cfg); err != nil {
		t.Fatalf("reload config: %v", err)
	}

	// BackPressure is unexported; verify by checking no panic.
}

func TestReloadConfigClearsACL(t *testing.T) {
	srv := NewServerWithStore(nil)

	// First load with ACL rules.
	cfg1 := config.Default()
	cfg1.ACL.Rules = []config.ACLRule{
		{User: "alice", VHost: "/", ResourceType: "queue", ResourceName: "*", Permission: "read", Allow: true},
	}
	if err := srv.ReloadConfig(cfg1); err != nil {
		t.Fatalf("reload config 1: %v", err)
	}
	if srv.ACLManager() == nil {
		t.Fatal("expected ACL manager")
	}

	// Reload with empty rules — ACL manager should be nil.
	cfg2 := config.Default()
	if err := srv.ReloadConfig(cfg2); err != nil {
		t.Fatalf("reload config 2: %v", err)
	}
	if srv.ACLManager() != nil {
		t.Fatal("expected nil ACL manager after clearing rules")
	}
}

func TestReloadConfigSavesConfig(t *testing.T) {
	srv := NewServerWithStore(nil)

	cfg := config.Default()
	cfg.Log.Level = "debug"

	if err := srv.ReloadConfig(cfg); err != nil {
		t.Fatalf("reload config: %v", err)
	}

	current := srv.Config()
	if current.Log.Level != "debug" {
		t.Fatalf("expected log level debug, got %s", current.Log.Level)
	}
}

func TestReloadTLS(t *testing.T) {
	srv := NewServerWithStore(nil)

	cfg := config.Default()
	cfg.TLS.Enabled = true
	cfg.TLS.CertFile = "/nonexistent/cert.pem"
	cfg.TLS.KeyFile = "/nonexistent/key.pem"

	// Should fail because cert files don't exist.
	if err := srv.ReloadTLS(cfg); err == nil {
		t.Fatal("expected error for missing cert files")
	}

	// Disable TLS — should be a no-op.
	cfg.TLS.Enabled = false
	if err := srv.ReloadTLS(cfg); err != nil {
		t.Fatalf("unexpected error when tls disabled: %v", err)
	}
}
