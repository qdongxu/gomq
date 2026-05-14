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
	"github.com/qdongxu/gomq/internal/server"
)

const version = "0.1.0"

func main() {
	configPath := flag.String(
		"config",
		"configs/gomq.default.toml",
		"path to TOML configuration file",
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := config.Validate(cfg); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	srv := server.NewServer()

	addr := "0.0.0.0:5672"
	if len(cfg.Network.Listeners) > 0 {
		addr = cfg.Network.Listeners[0]
	}

	if err := srv.Listen(addr); err != nil {
		log.Fatalf("listen: %v", err)
	}

	fmt.Printf("gomqd v%s started on %s\n", version, addr)
	fmt.Printf("heartbeat: %d s\n", cfg.Network.Heartbeat)

	go func() {
		if err := srv.Serve(); err != nil {
			log.Printf("serve: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("shutting down...")
	if err := srv.Shutdown(); err != nil {
		log.Printf("shutdown: %v", err)
	}
	fmt.Println("gomqd stopped")
}
