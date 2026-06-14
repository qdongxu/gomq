// gomqd is the gomq server entry point.
// It loads configuration, validates it, starts the server, and handles
// graceful shutdown on SIGINT or SIGTERM.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/qdongxu/gomq/internal/auth"
	"github.com/qdongxu/gomq/internal/cluster"
	"github.com/qdongxu/gomq/internal/config"
	"github.com/qdongxu/gomq/internal/metrics"
	"github.com/qdongxu/gomq/internal/server"
	"github.com/qdongxu/gomq/internal/store"
	"github.com/qdongxu/gomq/internal/web"
)

// Injected by -ldflags at build time.
var (
	version   = "dev"
	buildTime = "unknown"
)

func main() {
	configPath := flag.String(
		"config",
		"configs/gomq.default.toml",
		"path to TOML configuration file",
	)
	showVersion := flag.Bool(
		"version", false,
		"print version and exit",
	)
	snapshotFile := flag.String(
		"snapshot", "",
		"create a one-time snapshot and exit",
	)
	restoreFile := flag.String(
		"restore", "",
		"restore from snapshot file before starting",
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("gomqd %s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

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

	// Load ACL rules if any are configured.
	if len(cfg.ACL.Rules) > 0 {
		rules := make([]auth.Rule, 0, len(cfg.ACL.Rules))
		for _, r := range cfg.ACL.Rules {
			rules = append(rules, auth.Rule{
				User:         r.User,
				VHost:        r.VHost,
				ResourceType: auth.ResourceType(r.ResourceType),
				ResourceName: r.ResourceName,
				Permission:   auth.Permission(r.Permission),
				Allow:        r.Allow,
			})
		}
		srv.SetACLManager(auth.NewACLManager(rules))
		log.Printf("acl: loaded %d rules", len(rules))
	}

	if metaStore != nil {
		ctx, cancel := context.WithTimeout(
			context.Background(), 10*time.Second)
		if err := srv.RestoreFromStore(ctx); err != nil {
			log.Fatalf("restore from store: %v", err)
		}
		cancel()
	}

	// Restore from snapshot file if requested.
	if *restoreFile != "" {
		snap, err := server.LoadSnapshot(*restoreFile)
		if err != nil {
			log.Fatalf("load snapshot: %v", err)
		}
		if err := srv.RestoreFromSnapshot(snap); err != nil {
			log.Fatalf("restore from snapshot: %v", err)
		}
	}

	addr := "0.0.0.0:5672"
	if len(cfg.Network.Listeners) > 0 {
		addr = cfg.Network.Listeners[0]
	}

	if err := srv.Listen(addr); err != nil {
		log.Fatalf("listen: %v", err)
	}

	// TLS listener when configured.
	var tlsAddr string
	if cfg.TLS.Enabled {
		tlsCfg, err := server.NewTLSConfig(
			cfg.TLS.CertFile, cfg.TLS.KeyFile,
			cfg.TLS.CAFile, cfg.TLS.VerifyClient,
		)
		if err != nil {
			log.Fatalf("tls config: %v", err)
		}
		if len(cfg.TLS.CipherSuites) > 0 {
			if err := tlsCfg.SetCipherSuites(
				cfg.TLS.CipherSuites,
			); err != nil {
				log.Fatalf("cipher suites: %v", err)
			}
		}
		tlsAddr = "0.0.0.0:5671"
		if len(cfg.Network.Listeners) > 1 {
			tlsAddr = cfg.Network.Listeners[1]
		}
		if err := srv.ListenTLS(
			tlsAddr, tlsCfg.Config(),
		); err != nil {
			log.Fatalf("listen tls: %v", err)
		}
		srv.SetTLSCertPaths(
			cfg.TLS.CertFile, cfg.TLS.KeyFile,
			cfg.TLS.CAFile, cfg.TLS.VerifyClient,
		)
	}

	// Metrics endpoint when configured.
	if cfg.Metrics.Enabled {
		metricsAddr := "0.0.0.0:15692"
		if cfg.Metrics.Listen != "" {
			metricsAddr = cfg.Metrics.Listen
		}
		mc := metrics.NewPrometheusCollector()
		srv.SetMetrics(mc)
		mc.NodeUp()
		go func() {
			log.Printf("metrics endpoint: http://%s/metrics", metricsAddr)
			if err := mc.ListenAndServe(metricsAddr); err != nil {
				log.Printf("metrics server: %v", err)
			}
		}()
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
		srv.SetCluster(c)
		srv.SetMembership(m)
		log.Printf("cluster discovery enabled, node %s @ %s",
			cfg.Cluster.NodeID, addr)
	}

	fmt.Printf("gomqd v%s started on %s\n", version, addr)
	if tlsAddr != "" {
		fmt.Printf("tls listener: %s\n", tlsAddr)
	}
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

	// Config hot-reloader.
	var reloader *config.Reloader
	if *configPath != "" {
		reloader, err = config.NewReloader(
			*configPath,
			config.Load,
			func(newCfg *config.Config) error {
				return applyConfig(srv, cfg, newCfg)
			},
		)
		if err != nil {
			log.Printf("config watcher: %v", err)
		} else {
			reloader.Start()
			log.Printf("config hot-reload enabled: %s", *configPath)
		}
	}

	// Web UI when configured.
	var webBroker *server.WebBroker
	if cfg.Web.Enabled {
		webBroker = server.NewWebBroker(srv)
		web.SetBroker(webBroker)
		web.SetClusterBroker(webBroker)
		web.SetVHostBroker(webBroker)
		ws := web.NewServer(web.AuthConfig{
			Username: cfg.Web.Username,
			Password: cfg.Web.Password,
		})
		webAddr := "0.0.0.0:15672"
		if cfg.Web.Listen != "" {
			webAddr = cfg.Web.Listen
		}
		go func() {
			log.Printf("web UI: http://%s", webAddr)
			if err := http.ListenAndServe(webAddr, ws); err != nil {
				log.Printf("web server: %v", err)
			}
		}()
	}

	// Management endpoints (health, readiness, pprof).
	if cfg.Management.HealthEnabled {
		mgmtAddr := cfg.Management.BindAddress
		if mgmtAddr == "" {
			// Reuse web UI port when bind_address is empty.
			mgmtAddr = "0.0.0.0:15672"
			if cfg.Web.Listen != "" {
				mgmtAddr = cfg.Web.Listen
			}
		}
		mgmt := server.NewManagementServer(srv)
		if cfg.Management.PprofEnabled && cfg.Log.Level == "debug" {
			mgmt.EnablePprof()
			log.Printf("pprof enabled at http://%s/debug/pprof/", mgmtAddr)
		}
		go func() {
			log.Printf("health endpoint: http://%s/api/health", mgmtAddr)
			if err := mgmt.ListenAndServe(mgmtAddr); err != nil {
				log.Printf("management server: %v", err)
			}
		}()
	}

	// Start mirror queue background sync loop.
	var stopMirrorSync func()
	stopMirrorSync = srv.MirrorManager().StartSyncLoop(
		30 * time.Second,
	)

	// Initialise plugins.
	pluginNames := srv.PluginManager().LoadAll()
	if len(pluginNames) > 0 {
		srv.PluginManager().InitAll(srv)
		log.Printf("plugins loaded: %v", pluginNames)
	}

	// Start federation and shovel links (placeholder stubs).
	srv.FederationManager().StartAll()
	srv.ShovelManager().StartAll()

	// Snapshot manager for periodic snapshots.
	var snapMgr *server.SnapshotManager
	if cfg.Snapshot.Enabled {
		snapMgr = server.NewSnapshotManager(
			srv, cfg.Snapshot, cfg.Cluster.NodeID)
		snapMgr.Start()
	}

	// One-time snapshot mode (--snapshot <dir>).
	if *snapshotFile != "" {
		oneOff := server.NewSnapshotManager(
			srv, config.Snapshot{OutputDir: *snapshotFile}, cfg.Cluster.NodeID)
		if err := oneOff.Create(); err != nil {
			log.Fatalf("snapshot: %v", err)
		}
		fmt.Println("snapshot created")
		return
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

loop:
	for sig := range sigCh {
		switch sig {
		case syscall.SIGHUP:
			log.Println("received SIGHUP, reloading config...")
			if reloader != nil {
				if err := reloader.Reload(); err != nil {
					log.Printf("config reload failed: %v", err)
				}
			}
		default:
			break loop
		}
	}

	fmt.Println("shutting down...")
	if reloader != nil {
		reloader.Stop()
	}
	if snapMgr != nil {
		snapMgr.Stop()
	}
	if stopMirrorSync != nil {
		stopMirrorSync()
	}
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

// applyConfig compares the current and new configuration, applies
// reloadable changes to the server, and logs warnings for changes
// that require a restart.
func applyConfig(srv *server.Server, oldCfg, newCfg *config.Config) error {
	reloadable, ignored := config.IsReloadable(oldCfg, newCfg)
	if !reloadable {
		for _, key := range ignored {
			log.Printf("config reload: ignoring non-reloadable key %q (restart required)", key)
		}
	}

	// Apply server-level reloadable settings.
	if err := srv.ReloadConfig(newCfg); err != nil {
		return err
	}

	// Log level.
	if oldCfg.Log.Level != newCfg.Log.Level {
		log.Printf("config reload: log level changed %q -> %q", oldCfg.Log.Level, newCfg.Log.Level)
	}

	// TLS certificate paths.
	if newCfg.TLS.Enabled {
		if err := srv.ReloadTLS(newCfg); err != nil {
			return err
		}
		log.Printf("config reload: tls certificates updated")
	}

	return nil
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
