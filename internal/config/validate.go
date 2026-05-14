// validate.go checks a Config for structural and value correctness.
package config

import (
	"fmt"
	"net"
	"strings"
)

// Validate checks cfg and returns an error describing the first problem
// encountered.
func Validate(cfg *Config) error {
	if len(cfg.Network.Listeners) == 0 {
		return fmt.Errorf("network.listeners: must not be empty")
	}
	for _, addr := range cfg.Network.Listeners {
		if err := checkAddr(addr); err != nil {
			return fmt.Errorf(
				"network.listeners %q: %w", addr, err,
			)
		}
	}

	if cfg.Network.Heartbeat <= 0 {
		return fmt.Errorf(
			"network.heartbeat: must be > 0, got %d",
			cfg.Network.Heartbeat,
		)
	}

	if cfg.Network.FrameMax < 4096 {
		return fmt.Errorf(
			"network.frame_max: must be >= 4096, got %d",
			cfg.Network.FrameMax,
		)
	}

	if cfg.Network.ChannelMax < 1 {
		return fmt.Errorf(
			"network.channel_max: must be >= 1, got %d",
			cfg.Network.ChannelMax,
		)
	}

	if cfg.Web.Enabled && cfg.Web.Listen != "" {
		if err := checkAddr(cfg.Web.Listen); err != nil {
			return fmt.Errorf("web.listen: %w", err)
		}
	}

	if cfg.Cluster.NodeID == "" {
		return fmt.Errorf("cluster.node_id: must not be empty")
	}

	for _, node := range cfg.Cluster.Nodes {
		if node == "" {
			return fmt.Errorf("cluster.nodes: empty entry found")
		}
		// Each static node entry is expected as "node_id@host:port"
		if !strings.Contains(node, "@") {
			return fmt.Errorf(
				"cluster.nodes %q: missing '@' separator",
				node,
			)
		}
	}

	return nil
}

// checkAddr verifies that addr is a valid host:port string.
func checkAddr(addr string) error {
	_, _, err := net.SplitHostPort(addr)
	return err
}
