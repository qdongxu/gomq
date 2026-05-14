package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadDefaults checks that Load with an empty file falls back to
// safe defaults.
func TestLoadDefaults(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "empty.toml")
	if err := os.WriteFile(path, []byte{}, 0644); err != nil {
		t.Fatalf("write empty file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load empty config: %v", err)
	}

	if want := "0.0.0.0:5672"; cfg.Network.Listeners[0] != want {
		t.Errorf("default listener = %q, want %q",
			cfg.Network.Listeners[0], want)
	}
	if cfg.Network.Heartbeat != 60 {
		t.Errorf("default heartbeat = %d, want 60",
			cfg.Network.Heartbeat)
	}
	if cfg.Network.FrameMax != 131072 {
		t.Errorf("default frame_max = %d, want 131072",
			cfg.Network.FrameMax)
	}
	if cfg.Network.ChannelMax != 2048 {
		t.Errorf("default channel_max = %d, want 2048",
			cfg.Network.ChannelMax)
	}
	if cfg.Log.Level != "info" {
		t.Errorf("default log level = %q, want info",
			cfg.Log.Level)
	}
	if !cfg.Web.Enabled {
		t.Error("default web.enabled = false, want true")
	}
}

// TestLoadFromFile verifies that every field can be set from a TOML
// file.
func TestLoadFromFile(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "full.toml")
	data := []byte(`
[network]
listeners = ["127.0.0.1:5672"]
heartbeat = 30
frame_max = 65536
channel_max = 512

[log]
level = "debug"
output = "stderr"

[cluster]
node_id = "node-a"
discovery = "etcd"
nodes = ["node-b@192.168.1.2:5672"]

[web]
enabled = false
listen = "127.0.0.1:15672"
path_prefix = "/admin/"
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load full config: %v", err)
	}

	if cfg.Network.Listeners[0] != "127.0.0.1:5672" {
		t.Errorf("listener = %q", cfg.Network.Listeners[0])
	}
	if cfg.Network.Heartbeat != 30 {
		t.Errorf("heartbeat = %d", cfg.Network.Heartbeat)
	}
	if cfg.Network.FrameMax != 65536 {
		t.Errorf("frame_max = %d", cfg.Network.FrameMax)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("log level = %q", cfg.Log.Level)
	}
	if cfg.Cluster.NodeID != "node-a" {
		t.Errorf("node_id = %q", cfg.Cluster.NodeID)
	}
	if cfg.Web.Listen != "127.0.0.1:15672" {
		t.Errorf("web listen = %q", cfg.Web.Listen)
	}
	if cfg.Web.PathPrefix != "/admin/" {
		t.Errorf("path_prefix = %q", cfg.Web.PathPrefix)
	}
	if cfg.Web.Enabled {
		t.Error("web.enabled should be false")
	}
}

// TestEnvOverride confirms that environment variables take priority over
// file values.
func TestEnvOverride(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "base.toml")
	data := []byte(`[network]
heartbeat = 60
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	os.Setenv("GOMQ_NETWORK_HEARTBEAT", "120")
	os.Setenv("GOMQ_LOG_LEVEL", "warn")
	os.Setenv("GOMQ_WEB_ENABLED", "false")
	defer func() {
		os.Unsetenv("GOMQ_NETWORK_HEARTBEAT")
		os.Unsetenv("GOMQ_LOG_LEVEL")
		os.Unsetenv("GOMQ_WEB_ENABLED")
	}()

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load with env: %v", err)
	}

	if cfg.Network.Heartbeat != 120 {
		t.Errorf("env override heartbeat = %d, want 120",
			cfg.Network.Heartbeat)
	}
	if cfg.Log.Level != "warn" {
		t.Errorf("env override log level = %q, want warn",
			cfg.Log.Level)
	}
	if cfg.Web.Enabled {
		t.Error("env override web.enabled should be false")
	}
}

// TestValidateErrors exercises every validation failure path.
func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name string
		fn   func(*Config)
		want string
	}{
		{
			name: "empty listeners",
			fn: func(c *Config) {
				c.Network.Listeners = nil
			},
			want: "network.listeners",
		},
		{
			name: "bad listener address",
			fn: func(c *Config) {
				c.Network.Listeners = []string{"not-an-address"}
			},
			want: "network.listeners",
		},
		{
			name: "zero heartbeat",
			fn: func(c *Config) {
				c.Network.Heartbeat = 0
			},
			want: "network.heartbeat",
		},
		{
			name: "small frame_max",
			fn: func(c *Config) {
				c.Network.FrameMax = 1000
			},
			want: "network.frame_max",
		},
		{
			name: "zero channel_max",
			fn: func(c *Config) {
				c.Network.ChannelMax = 0
			},
			want: "network.channel_max",
		},
		{
			name: "bad web listen",
			fn: func(c *Config) {
				c.Web.Listen = "bad"
			},
			want: "web.listen",
		},
		{
			name: "empty node_id",
			fn: func(c *Config) {
				c.Cluster.NodeID = ""
			},
			want: "cluster.node_id",
		},
		{
			name: "bad cluster node",
			fn: func(c *Config) {
				c.Cluster.Nodes = []string{"no-at-sign"}
			},
			want: "cluster.nodes",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Default()
			tc.fn(cfg)
			err := Validate(cfg)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want substring %q",
					err.Error(), tc.want)
			}
		})
	}
}

// TestValidateSuccess ensures a well-formed default config passes.
func TestValidateSuccess(t *testing.T) {
	cfg := Default()
	if err := Validate(cfg); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
}
