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

// TLS holds TLS/SSL listener configuration.
type TLS struct {
	Enabled      bool     `toml:"enabled"`       // enable TLS listener
	CertFile     string   `toml:"cert_file"`      // path to server certificate
	KeyFile      string   `toml:"key_file"`       // path to server private key
	CAFile       string   `toml:"ca_file"`        // path to CA cert for client verify
	VerifyClient bool     `toml:"verify_client"`  // require client cert (mTLS)
	CipherSuites []string `toml:"cipher_suites"` // optional cipher suite names
}

// Log holds logging configuration.
type Log struct {
	Level  string `toml:"level"`   // debug, info, warn, error
	Output string `toml:"output"`  // stdout, stderr, or file path
}

// Cluster holds node identity and peer discovery settings.
type Cluster struct {
	NodeID        string   `toml:"node_id"`       // unique node identifier
	Discovery     string   `toml:"discovery"`     // "static" or "etcd"
	Nodes         []string `toml:"nodes"`          // peer addresses when discovery=static
	EtcdEndpoints []string `toml:"etcd_endpoints"` // etcd server addresses
}

// Web holds the management UI configuration.
type Web struct {
	Enabled    bool   `toml:"enabled"`     // whether the web UI is active
	Listen     string `toml:"listen"`      // HTTP bind address,
		// e.g. "0.0.0.0:15672"
	PathPrefix string `toml:"path_prefix"` // URL prefix for reverse-proxy support
}

// Metrics holds Prometheus metrics export configuration.
type Metrics struct {
	Enabled bool   `toml:"enabled"` // enable /metrics endpoint
	Listen  string `toml:"listen"`  // HTTP bind address,
		// e.g. "0.0.0.0:15692"
}

// Memory holds message-store tuning settings.
type Memory struct {
	CompressionThreshold int    `toml:"compression_threshold"` // min payload bytes to compress, 0=off
	MaxInMemoryMessages  int    `toml:"max_in_memory_messages"` // max messages per queue before paging, 0=off
	PageDir              string `toml:"page_dir"` // directory for overflow page files
}

// Limits holds rate limiting and backpressure settings.
type Limits struct {
	MaxConnectionsPerSecond float64 `toml:"max_connections_per_second"` // 0 = no limit
	MemoryThresholdPercent  float64 `toml:"memory_threshold_percent"`   // 0 = no backpressure
	BackPressureEnabled     bool    `toml:"backpressure_enabled"`       // master switch
}

// Auth is a placeholder for authentication settings.
// Real implementation will be added in a later Issue.
type Auth struct {
	// intentionally empty for now
}

// ACLRule is a single access-control entry.
type ACLRule struct {
	User         string `toml:"user"`
	VHost        string `toml:"vhost"`
	ResourceType string `toml:"resource_type"`
	ResourceName string `toml:"resource_name"`
	Permission   string `toml:"permission"`
	Allow        bool   `toml:"allow"`
}

// ACL holds the access-control list configuration.
type ACL struct {
	Rules []ACLRule `toml:"rules"`
}

// Config is the top-level server configuration.
type Config struct {
	Network Network `toml:"network"`
	Log     Log     `toml:"log"`
	Cluster Cluster `toml:"cluster"`
	Web     Web     `toml:"web"`
	Auth    Auth    `toml:"auth"`
	ACL     ACL     `toml:"acl"`
	TLS     TLS     `toml:"tls"`
	Metrics Metrics `toml:"metrics"`
	Memory  Memory  `toml:"memory"`
	Limits  Limits  `toml:"limits"`
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
		Memory: Memory{
			CompressionThreshold: 0,
			MaxInMemoryMessages:  0,
			PageDir:              "/var/lib/gomq/pages",
		},
	}
}
