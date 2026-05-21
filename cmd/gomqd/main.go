// gomqd is the gomq server entry point.
// It loads configuration, validates it, starts the server, and handles
// graceful shutdown on SIGINT or SIGTERM.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/qdongxu/gomq/internal/cluster"
	"github.com/qdongxu/gomq/internal/config"
	"github.com/qdongxu/gomq/internal/server"
	"github.com/qdongxu/gomq/internal/store"
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

	metaStore, closeStore, err := newStore(cfg)
	if err != nil {
		log.Fatalf("init store: %v", err)
	}
	if closeStore != nil {
		defer closeStore()
	}

	srv := server.NewServerWithStore(metaStore)

	if metaStore != nil {
		ctx, cancel := context.WithTimeout(
			context.Background(), 10*time.Second)
		if err := srv.RestoreFromStore(ctx); err != nil {
			log.Fatalf("restore from store: %v", err)
		}
		cancel()
	}

	addr := "0.0.0.0:5672"
	if len(cfg.Network.Listeners) > 0 {
		addr = cfg.Network.Listeners[0]
	}

	if err := srv.Listen(addr); err != nil {
		log.Fatalf("listen: %v", err)
	}

	// Cluster discovery via etcd when configured.
	var (
		discovery  *cluster.Discovery
		membership *cluster.Membership
		gossip     *cluster.Gossip
	)
	if len(cfg.Cluster.EtcdEndpoints) > 0 {
		etcdClient, err := clientv3.New(clientv3.Config{
			Endpoints:   cfg.Cluster.EtcdEndpoints,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			log.Fatalf("etcd client: %v", err)
		}
		defer func() { _ = etcdClient.Close() }()

		c, d, m, g, err := cluster.NewClusterWithDiscovery(
			etcdClient, cfg.Cluster.NodeID, addr)
		if err != nil {
			log.Fatalf("cluster discovery: %v", err)
		}
		_ = c
		discovery = d
		membership = m
		gossip = g
		log.Printf("cluster discovery enabled, node %s @ %s",
			cfg.Cluster.NodeID, addr)
	}

	fmt.Printf("gomqd v%s started on %s\n", version, addr)
	fmt.Printf("heartbeat: %d s\n", cfg.Network.Heartbeat)
	if membership != nil {
		fmt.Printf("cluster members: %d online\n",
			membership.OnlineCount())
	}

	go func() {
		if err := srv.Serve(); err != nil {
			log.Printf("serve: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("shutting down...")
	if gossip != nil {
		gossip.Stop()
	}
	if discovery != nil {
		ctx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second)
		_ = discovery.Deregister(ctx)
		cancel()
	}
	if err := srv.Shutdown(); err != nil {
		log.Printf("shutdown: %v", err)
	}
	fmt.Println("gomqd stopped")
}

// newStore creates a persistence backend based on configuration.
func newStore(cfg *config.Config) (
	store.Store,
	func(),
	error,
) {
	if len(cfg.Cluster.EtcdEndpoints) > 0 {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   cfg.Cluster.EtcdEndpoints,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return nil, nil, fmt.Errorf(
				"etcd client: %w", err)
		}
		return store.NewEtcdStore(client, "/gomq"),
			func() { _ = client.Close() },
			nil
	}
	return store.NewMemoryStore(), nil, nil
}
