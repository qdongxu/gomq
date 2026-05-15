// cluster.go provides the foundation for gomq clustering.
package cluster

import (
	"sync"
	"time"
)

// Node represents a single broker node in the cluster.
type Node struct {
	ID       string
	Addr     string
	LastSeen time.Time
}

// Cluster tracks all active nodes and provides leader election.
type Cluster struct {
	localID   string
	localAddr string
	nodes     map[string]*Node
	leader    string
	mu        sync.RWMutex
}

// NewCluster creates a cluster with the local node registered.
func NewCluster(localID, localAddr string) *Cluster {
	c := &Cluster{
		localID:   localID,
		localAddr: localAddr,
		nodes:     make(map[string]*Node),
	}
	c.nodes[localID] = &Node{
		ID:       localID,
		Addr:     localAddr,
		LastSeen: time.Now(),
	}
	c.leader = localID
	return c
}

// Join adds a remote node to the cluster.
func (c *Cluster) Join(id, addr string) {
	c.mu.Lock()
	c.nodes[id] = &Node{
		ID:       id,
		Addr:     addr,
		LastSeen: time.Now(),
	}
	c.mu.Unlock()
}

// Leave removes a node from the cluster.
func (c *Cluster) Leave(id string) {
	c.mu.Lock()
	delete(c.nodes, id)
	if c.leader == id {
		c.leader = c.localID
	}
	c.mu.Unlock()
}

// Nodes returns a snapshot of all active nodes.
func (c *Cluster) Nodes() []*Node {
	c.mu.RLock()
	out := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		out = append(out, n)
	}
	c.mu.RUnlock()
	return out
}

// NodeCount returns the number of nodes in the cluster.
func (c *Cluster) NodeCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.nodes)
}

// Leader returns the current leader node ID.
func (c *Cluster) Leader() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leader
}

// IsLeader reports whether the local node is the leader.
func (c *Cluster) IsLeader() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leader == c.localID
}

// LocalID returns the local node ID.
func (c *Cluster) LocalID() string { return c.localID }

// LocalAddr returns the local node address.
func (c *Cluster) LocalAddr() string { return c.localAddr }

// Heartbeat updates the last-seen timestamp for a node.
func (c *Cluster) Heartbeat(id string) {
	c.mu.Lock()
	if n, ok := c.nodes[id]; ok {
		n.LastSeen = time.Now()
	}
	c.mu.Unlock()
}
