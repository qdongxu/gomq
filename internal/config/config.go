// Package config provides TOML-based server configuration with defaults,
// environment-variable overrides, and validation.
package config

// Network holds listener and protocol tuning settings.
type Network struct {
	Listeners   []string `toml:"listeners"`    // TCP bind addresses,
		// e.g. ["0.0.0.0:5672"]
	Heartbeat   int      `toml:"heartbeat"`    // seconds between heartbeats, > 0
	FrameMax    int      `toml:"frame_max"`    // max frame size in bytes, >= 4096
	ChannelMax  int      `toml:"channel_max"`  // max channels per connection, >= 1
}

// Log holds logging configuration.
type Log struct {
	Level  string `toml:"level"`   // debug, info, warn, error
	Output string `toml:"output"`  // stdout, stderr, or file path
}

// Cluster holds node identity and peer discovery settings.
type Cluster struct {
	NodeID    string   `toml:"node_id"`    // unique node identifier
	Discovery string   `toml:"discovery"`  // "static" or "etcd"
	Nodes     []string `toml:"nodes"`     // peer addresses when discovery=static
}

// Web holds the management UI configuration.
type Web struct {
	Enabled    bool   `toml:"enabled"`     // whether the web UI is active
	Listen     string `toml:"listen"`      // HTTP bind address,
		// e.g. "0.0.0.0:15672"
	PathPrefix string `toml:"path_prefix"` // URL prefix for reverse-proxy support
}

// Auth is a placeholder for authentication settings.
// Real implementation will be added in a later Issue.
type Auth struct {
	// intentionally empty for now
}

// Config is the top-level server configuration.
type Config struct {
	Network Network `toml:"network"`
	Log     Log     `toml:"log"`
	Cluster Cluster `toml:"cluster"`
	Web     Web     `toml:"web"`
	Auth    Auth    `toml:"auth"`
}

// Default returns a Config populated with safe defaults.
func Default() *Config {
	return &Config{
		Network: Network{
			Listeners:  []string{"0.0.0.0:5672"},
			Heartbeat:  60,
			FrameMax:   131072,
			ChannelMax: 2048,
		},
		Log: Log{
			Level:  "info",
			Output: "stdout",
		},
		Cluster: Cluster{
			NodeID:    "node-1",
			Discovery: "static",
		},
		Web: Web{
			Enabled:    true,
			Listen:     "0.0.0.0:15672",
			PathPrefix: "/",
		},
	}
}
