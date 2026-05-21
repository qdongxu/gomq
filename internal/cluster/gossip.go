// gossip.go propagates cluster membership events via etcd watch.
package cluster

import (
	"context"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Gossip watches etcd for node changes and updates Membership.
type Gossip struct {
	discovery  *Discovery
	membership *Membership
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewGossip creates a gossip instance bound to discovery and
// membership.
func NewGossip(
	d *Discovery,
	m *Membership,
) *Gossip {
	ctx, cancel := context.WithCancel(context.Background())
	return &Gossip{
		discovery:  d,
		membership: m,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start begins watching etcd for membership events.
func (g *Gossip) Start() {
	go g.loop()
}

// Stop cancels the watch loop.
func (g *Gossip) Stop() {
	g.cancel()
}

func (g *Gossip) loop() {
	// Wait a moment for the initial registration to complete.
	time.Sleep(100 * time.Millisecond)

	g.discovery.Discover(g.ctx, func(ev Event) {
		switch ev.Type {
		case EventPut:
			g.membership.Join(ev.NodeID, ev.Addr)
			log.Printf("node joined: %s @ %s", ev.NodeID, ev.Addr)
		case EventDelete:
			g.membership.Leave(ev.NodeID)
			log.Printf("node left: %s", ev.NodeID)
		}
	})
}

// NewClusterWithDiscovery bootstraps a cluster using etcd discovery.
func NewClusterWithDiscovery(
	client *clientv3.Client,
	localID, localAddr string,
) (*Cluster, *Discovery, *Membership, *Gossip, error) {
	c := NewCluster(localID, localAddr)
	d := NewDiscovery(client, localID, localAddr)
	m := NewMembership()
	g := NewGossip(d, m)

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Register(ctx); err != nil {
		return nil, nil, nil, nil, err
	}

	m.Join(localID, localAddr)
	g.Start()
	return c, d, m, g, nil
}
