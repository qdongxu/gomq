// gomqd is the gomq server entry point.
// It loads configuration, validates it, starts the server, and handles
// graceful shutdown on SIGINT or SIGTERM.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/qdongxu/gomq/internal/config"
)

const version = "0.1.0"

// Server represents the gomqd process.
type Server struct {
	cfg *config.Config
}

// NewServer loads and validates configuration, then creates a Server.
func NewServer(configPath string) (*Server, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	if err := config.Validate(cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &Server{cfg: cfg}, nil
}

// Run starts the server and blocks until a shutdown signal is received.
func (s *Server) Run() error {
	fmt.Printf("gomqd v%s started\n", version)
	fmt.Printf("listeners: %v\n", s.cfg.Network.Listeners)
	fmt.Printf("heartbeat: %d s\n", s.cfg.Network.Heartbeat)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("gomqd stopped")
	return nil
}

func main() {
	configPath := flag.String(
		"config",
		"configs/gomq.default.toml",
		"path to TOML configuration file",
	)
	flag.Parse()

	srv, err := NewServer(*configPath)
	if err != nil {
		log.Fatalf("failed to start: %v", err)
	}

	if err := srv.Run(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
