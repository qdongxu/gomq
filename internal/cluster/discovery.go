// discovery.go provides etcd-based node registration and discovery.
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	nodesPrefix    = "/gomq/nodes/"
	heartbeatTTL   = 10 // seconds
	dialTimeout    = 5 * time.Second
)

// NodeInfo is the serializable node metadata stored in etcd.
type NodeInfo struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// Discovery registers the local node and watches for peers.
type Discovery struct {
	client  *clientv3.Client
	localID string
	addr    string
	leaseID clientv3.LeaseID
	stopCh  chan struct{}
}

// NewDiscovery creates a discovery client for the local node.
func NewDiscovery(
	client *clientv3.Client,
	localID, addr string,
) *Discovery {
	return &Discovery{
		client:  client,
		localID: localID,
		addr:    addr,
		stopCh:  make(chan struct{}),
	}
}

// Register creates a lease and writes the local node record.
func (d *Discovery) Register(ctx context.Context) error {
	resp, err := d.client.Grant(ctx, heartbeatTTL)
	if err != nil {
		return fmt.Errorf("grant lease: %w", err)
	}
	d.leaseID = resp.ID

	data, _ := json.Marshal(NodeInfo{ID: d.localID, Addr: d.addr})
	key := nodesPrefix + d.localID
	_, err = d.client.Put(ctx, key, string(data),
		clientv3.WithLease(d.leaseID))
	if err != nil {
		return fmt.Errorf("put node: %w", err)
	}

	go d.keepAlive()
	return nil
}

// keepAlive sends periodic lease keep-alive requests.
func (d *Discovery) keepAlive() {
	ch, err := d.client.KeepAlive(context.Background(), d.leaseID)
	if err != nil {
		log.Printf("keepalive start: %v", err)
		return
	}
	for {
		select {
		case <-d.stopCh:
			return
		case ka, ok := <-ch:
			if !ok {
				log.Println("keepalive channel closed")
				return
			}
			_ = ka
		}
	}
}

// Deregister revokes the lease, removing the node record.
func (d *Discovery) Deregister(ctx context.Context) error {
	close(d.stopCh)
	_, err := d.client.Revoke(ctx, d.leaseID)
	return err
}

// Discover watches the nodes prefix and invokes cb for every change.
func (d *Discovery) Discover(ctx context.Context, cb EventCallback) {
	watchCh := d.client.Watch(ctx, nodesPrefix,
		clientv3.WithPrefix())
	for wresp := range watchCh {
		for _, ev := range wresp.Events {
			var info NodeInfo
			if len(ev.Kv.Value) > 0 {
				_ = json.Unmarshal(ev.Kv.Value, &info)
			}
			event := Event{
				Type:   eventType(int32(ev.Type)),
				NodeID: info.ID,
				Addr:   info.Addr,
			}
			cb(event)
		}
	}
}

// EventCallback is invoked when a node joins or leaves.
type EventCallback func(Event)

// EventType describes a membership change.
type EventType int

const (
	EventPut EventType = iota
	EventDelete
)

// Event carries a single membership change.
type Event struct {
	Type   EventType
	NodeID string
	Addr   string
}

func eventType(t int32) EventType {
	if t == 1 { // mvccpb.DELETE
		return EventDelete
	}
	return EventPut
}
