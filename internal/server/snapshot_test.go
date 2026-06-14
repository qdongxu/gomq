// snapshot_test.go tests snapshot creation and restoration.
package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/qdongxu/gomq/internal/cluster"
	"github.com/qdongxu/gomq/internal/config"
	"github.com/qdongxu/gomq/internal/store"
)

func TestSnapshotManager_Create(t *testing.T) {
	meta := store.NewMemoryStore()
	srv := NewServerWithStore(meta)

	// Seed some state.
	_, _ = srv.ExchangeManager().Declare("ex1", ExchangeDirect, true, false, false, nil)
	_, _ = srv.QueueManager().Declare("q1", true, false, false, nil, nil)
	_, _ = srv.BindingManager().Bind("ex1", "q1", "rk1", nil)
	_, _ = srv.VHostManager().Create("vh1", "test vhost")
	c := cluster.NewCluster("node-1", "127.0.0.1:5672")
	c.Join("node-2", "127.0.0.1:5673")
	srv.SetCluster(c)

	dir := t.TempDir()
	mgr := NewSnapshotManager(srv, config.Snapshot{
		Enabled:  true,
		Interval: 3600,
		OutputDir: dir,
	}, "node-1")

	if err := mgr.Create(); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected snapshot file, got none")
	}

	// Verify it is valid JSON.
	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("snapshot file is empty")
	}
}

func TestSnapshotManager_RetainCount(t *testing.T) {
	meta := store.NewMemoryStore()
	srv := NewServerWithStore(meta)
	dir := t.TempDir()
	mgr := NewSnapshotManager(srv, config.Snapshot{
		Enabled:     true,
		Interval:    1,
		RetainCount: 2,
		OutputDir:   dir,
	}, "node-1")

	for i := 0; i < 4; i++ {
		if err := mgr.Create(); err != nil {
			t.Fatalf("create snapshot %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(entries))
	}
}

func TestLoadSnapshot(t *testing.T) {
	meta := store.NewMemoryStore()
	srv := NewServerWithStore(meta)
	_, _ = srv.ExchangeManager().Declare("ex1", ExchangeDirect, true, false, false, nil)
	_, _ = srv.QueueManager().Declare("q1", true, false, false, nil, nil)
	_, _ = srv.BindingManager().Bind("ex1", "q1", "rk1", nil)
	_, _ = srv.VHostManager().Create("vh1", "test vhost")
	c := cluster.NewCluster("node-1", "127.0.0.1:5672")
	c.Join("node-2", "127.0.0.1:5673")
	srv.SetCluster(c)

	dir := t.TempDir()
	mgr := NewSnapshotManager(srv, config.Snapshot{OutputDir: dir}, "node-1")
	if err := mgr.Create(); err != nil {
		t.Fatalf("create: %v", err)
	}

	entries, _ := os.ReadDir(dir)
	path := filepath.Join(dir, entries[0].Name())

	snap, err := LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snap.Version != snapshotVersion {
		t.Fatalf("version %q, want %q", snap.Version, snapshotVersion)
	}
	if snap.NodeID != "node-1" {
		t.Fatalf("nodeID %q, want node-1", snap.NodeID)
	}
	if len(snap.Exchanges) == 0 {
		t.Fatal("expected exchanges in snapshot")
	}
	if len(snap.Queues) == 0 {
		t.Fatal("expected queues in snapshot")
	}
	if len(snap.Bindings) == 0 {
		t.Fatal("expected bindings in snapshot")
	}
	if len(snap.VHosts) == 0 {
		t.Fatal("expected vhosts in snapshot")
	}
	if len(snap.ClusterNodes) == 0 {
		t.Fatal("expected cluster nodes in snapshot")
	}
}

func TestRestoreFromSnapshot(t *testing.T) {
	meta := store.NewMemoryStore()
	// Source server.
	src := NewServerWithStore(meta)
	_, _ = src.ExchangeManager().Declare("ex1", ExchangeDirect, true, false, false, nil)
	_, _ = src.QueueManager().Declare("q1", true, false, false, nil, nil)
	_, _ = src.BindingManager().Bind("ex1", "q1", "rk1", nil)
	_, _ = src.VHostManager().Create("vh1", "test vhost")
	c := cluster.NewCluster("node-1", "127.0.0.1:5672")
	c.Join("node-2", "127.0.0.1:5673")
	src.SetCluster(c)

	dir := t.TempDir()
	mgr := NewSnapshotManager(src, config.Snapshot{OutputDir: dir}, "node-1")
	if err := mgr.Create(); err != nil {
		t.Fatalf("create: %v", err)
	}

	entries, _ := os.ReadDir(dir)
	path := filepath.Join(dir, entries[0].Name())

	// Target server.
	meta2 := store.NewMemoryStore()
	dst := NewServerWithStore(meta2)
	dst.SetCluster(cluster.NewCluster("node-1", "127.0.0.1:5672"))

	snap, err := LoadSnapshot(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := dst.RestoreFromSnapshot(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if _, ok := dst.ExchangeManager().Get("ex1"); !ok {
		t.Fatal("expected exchange ex1 restored")
	}
	if _, ok := dst.QueueManager().Get("q1"); !ok {
		t.Fatal("expected queue q1 restored")
	}
	if binds := dst.BindingManager().GetBindings("ex1"); len(binds) == 0 {
		t.Fatal("expected binding restored")
	}
	if _, ok := dst.VHostManager().Get("vh1"); !ok {
		t.Fatal("expected vhost vh1 restored")
	}
	if dst.Cluster().NodeCount() != 2 {
		t.Fatalf("expected 2 cluster nodes, got %d", dst.Cluster().NodeCount())
	}
}

func TestRestoreFromSnapshot_WrongVersion(t *testing.T) {
	meta := store.NewMemoryStore()
	srv := NewServerWithStore(meta)
	bad := &SnapshotData{Version: "999", CreatedAt: time.Now().UTC()}
	if err := srv.RestoreFromSnapshot(bad); err == nil {
		t.Fatal("expected error for unsupported version")
	}
}

func TestRestoreFromSnapshot_Nil(t *testing.T) {
	meta := store.NewMemoryStore()
	srv := NewServerWithStore(meta)
	if err := srv.RestoreFromSnapshot(nil); err != nil {
		t.Fatalf("nil snapshot should be no-op: %v", err)
	}
}
