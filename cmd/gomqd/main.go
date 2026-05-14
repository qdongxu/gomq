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
	"path/filepath"
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

// ConfigResolver finds the configuration file when the user does not
// provide an explicit path.
type ConfigResolver struct {
	defaultFile string
}

// NewConfigResolver creates a resolver with the given default filename.
func NewConfigResolver(defaultFile string) *ConfigResolver {
	return &ConfigResolver{defaultFile: defaultFile}
}

// Resolve returns the configuration file path.
// It checks, in order:
//   1. The flag value if explicitly set.
//   2. The first positional argument.
//   3. The default file in the current working directory.
//   4. The default file next to the executable.
//   5. The default file in the executable parent directory.
func (r *ConfigResolver) Resolve(
	flagPath string,
	explicit bool,
	args []string,
) (string, error) {
	if explicit && flagPath != "" {
		return flagPath, nil
	}
	if len(args) > 0 {
		return args[0], nil
	}

	candidates := []string{
		filepath.Join(".", "configs", r.defaultFile),
	}

	exe, err := os.Executable()
	if err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(exeDir, "configs", r.defaultFile),
			filepath.Join(exeDir, "..", "configs", r.defaultFile),
		)
	}

	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}

	return "", fmt.Errorf(
		"cannot find config file; looked in %v",
		candidates,
	)
}

func main() {
	configPath := flag.String(
		"config",
		"",
		"path to TOML configuration file",
	)
	flag.Parse()

	resolver := NewConfigResolver("gomq.default.toml")
	path, err := resolver.Resolve(
		*configPath,
		*configPath != "",
		flag.Args(),
	)
	if err != nil {
		log.Fatalf("failed to locate config: %v", err)
	}

	srv, err := NewServer(path)
	if err != nil {
		log.Fatalf("failed to start: %v", err)
	}

	if err := srv.Run(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
