// snapshot.go implements snapshot creation and management for broker state.
package server

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/qdongxu/gomq/internal/config"
)

const snapshotVersion = "1"

// SnapshotData holds a complete snapshot of broker metadata.
type SnapshotData struct {
	Version      string            `json:"version"`
	CreatedAt    time.Time         `json:"created_at"`
	NodeID       string            `json:"node_id"`
	Exchanges    []ExchangeSnap    `json:"exchanges"`
	Queues       []QueueSnap       `json:"queues"`
	Bindings     []BindingSnap     `json:"bindings"`
	VHosts       []VHostSnap       `json:"vhosts"`
	ClusterNodes []ClusterNodeSnap `json:"cluster_nodes"`
}

// ExchangeSnap is the serializable form of an exchange.
type ExchangeSnap struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	Durable    bool                   `json:"durable"`
	AutoDelete bool                   `json:"auto_delete"`
	Internal   bool                   `json:"internal"`
	Args       map[string]interface{} `json:"args,omitempty"`
}

// QueueSnap is the serializable form of a queue.
type QueueSnap struct {
	Name       string                 `json:"name"`
	Durable    bool                   `json:"durable"`
	Exclusive  bool                   `json:"exclusive"`
	AutoDelete bool                   `json:"auto_delete"`
	Args       map[string]interface{} `json:"args,omitempty"`
}

// BindingSnap is the serializable form of a binding.
type BindingSnap struct {
	Exchange   string                 `json:"exchange"`
	Queue      string                 `json:"queue"`
	RoutingKey string                 `json:"routing_key"`
	Args       map[string]interface{} `json:"args,omitempty"`
}

// VHostSnap is the serializable form of a VHost.
type VHostSnap struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
}

// ClusterNodeSnap is the serializable form of a cluster node.
type ClusterNodeSnap struct {
	ID       string    `json:"id"`
	Addr     string    `json:"addr"`
	LastSeen time.Time `json:"last_seen"`
}

// SnapshotManager creates periodic snapshots of broker state.
type SnapshotManager struct {
	server     *Server
	cfg        config.Snapshot
	nodeID     string
	mu         sync.Mutex
	stopCh     chan struct{}
	stopped    bool
}

// NewSnapshotManager creates a snapshot manager for the given server.
func NewSnapshotManager(
	srv *Server,
	cfg config.Snapshot,
	nodeID string,
) *SnapshotManager {
	return &SnapshotManager{
		server: srv,
		cfg:    cfg,
		nodeID: nodeID,
		stopCh: make(chan struct{}),
	}
}

// Start begins the periodic snapshot goroutine.
func (m *SnapshotManager) Start() {
	if m.cfg.Interval <= 0 {
		return
	}
	go m.loop()
}

// Stop signals the snapshot loop to exit.
func (m *SnapshotManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}
	m.stopped = true
	close(m.stopCh)
}

func (m *SnapshotManager) loop() {
	ticker := time.NewTicker(time.Duration(m.cfg.Interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := m.Create(); err != nil {
				log.Printf("snapshot failed: %v", err)
			}
		case <-m.stopCh:
			return
		}
	}
}

// Create captures the current broker state and writes it to a file.
func (m *SnapshotManager) Create() error {
	data := m.capture()

	out, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	outDir := m.cfg.OutputDir
	if outDir == "" {
		outDir = "."
	}
	if err := os.MkdirAll(outDir, 0750); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}

	ts := time.Now().UTC().Format("20060102_150405.000")
	fname := fmt.Sprintf("gomq_snapshot_%s_%s.json", m.nodeID, ts)
	fpath := filepath.Join(outDir, fname)

	if err := os.WriteFile(fpath, out, 0640); err != nil {
		return fmt.Errorf("write snapshot %s: %w", fpath, err)
	}

	log.Printf("snapshot created: %s", fpath)

	if m.cfg.RetainCount > 0 {
		if err := m.retain(outDir); err != nil {
			log.Printf("snapshot retain cleanup: %v", err)
		}
	}

	return nil
}

func (m *SnapshotManager) capture() *SnapshotData {
	srv := m.server

	// Exchanges.
	var exchanges []ExchangeSnap
	for _, ex := range srv.ExchangeManager().List() {
		exchanges = append(exchanges, ExchangeSnap{
			Name:       ex.Name,
			Type:       string(ex.Type),
			Durable:    ex.Durable,
			AutoDelete: ex.AutoDelete,
			Internal:   ex.Internal,
			Args:       ex.Args,
		})
	}

	// Queues.
	var queues []QueueSnap
	for _, q := range srv.QueueManager().List() {
		queues = append(queues, QueueSnap{
			Name:       q.Name,
			Durable:    q.Durable,
			Exclusive:  q.Exclusive,
			AutoDelete: q.AutoDelete,
			Args:       q.Args,
		})
	}

	// Bindings.
	var bindings []BindingSnap
	for _, b := range srv.BindingManager().ListAll() {
		bindings = append(bindings, BindingSnap{
			Exchange:   b.ExchangeName,
			Queue:      b.QueueName,
			RoutingKey: b.RoutingKey,
			Args:       b.Args,
		})
	}

	// VHosts.
	var vhosts []VHostSnap
	for _, vh := range srv.VHostManager().List() {
		vhosts = append(vhosts, VHostSnap{
			Name:        vh.Name,
			Description: vh.Description,
			CreatedAt:   vh.CreatedAt,
		})
	}

	// Cluster nodes.
	var nodes []ClusterNodeSnap
	if c := srv.Cluster(); c != nil {
		for _, n := range c.Nodes() {
			nodes = append(nodes, ClusterNodeSnap{
				ID:       n.ID,
				Addr:     n.Addr,
				LastSeen: n.LastSeen,
			})
		}
	}

	return &SnapshotData{
		Version:      snapshotVersion,
		CreatedAt:    time.Now().UTC(),
		NodeID:       m.nodeID,
		Exchanges:    exchanges,
		Queues:       queues,
		Bindings:     bindings,
		VHosts:       vhosts,
		ClusterNodes: nodes,
	}
}

func (m *SnapshotManager) retain(outDir string) error {
	entries, err := os.ReadDir(outDir)
	if err != nil {
		return err
	}

	var snaps []os.DirEntry
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == ".json" {
			snaps = append(snaps, e)
		}
	}

	if len(snaps) <= m.cfg.RetainCount {
		return nil
	}

	// Sort by ModTime (oldest first).
	sort.Slice(snaps, func(i, j int) bool {
		fi, _ := snaps[i].Info()
		fj, _ := snaps[j].Info()
		return fi.ModTime().Before(fj.ModTime())
	})

	for i := 0; i < len(snaps)-m.cfg.RetainCount; i++ {
		old := filepath.Join(outDir, snaps[i].Name())
		if err := os.Remove(old); err != nil {
			log.Printf("remove old snapshot %s: %v", old, err)
		}
	}

	return nil
}

// LoadSnapshot reads a snapshot file from disk.
func LoadSnapshot(path string) (*SnapshotData, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read snapshot %s: %w", path, err)
	}
	var snap SnapshotData
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	return &snap, nil
}
