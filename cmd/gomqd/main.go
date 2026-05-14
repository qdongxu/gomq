// gomqd is the gomq server entry point.
// It loads configuration, starts the server, and handles graceful
// shutdown on SIGINT or SIGTERM.
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

// NewServer creates a Server from the given config path.
func NewServer(configPath string) (*Server, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}
	return &Server{cfg: cfg}, nil
}

// Run starts the server and blocks until a shutdown signal
// is received.
func (s *Server) Run() error {
	fmt.Printf("gomqd v%s started\n", version)
	fmt.Printf("config path: %s\n", s.cfg.ConfigPath)

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
		log.Fatalf("failed to load config: %v", err)
	}

	if err := srv.Run(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
