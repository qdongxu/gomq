// restore.go implements snapshot-based state restoration.
package server

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/qdongxu/gomq/internal/store"
)

// RestoreFromSnapshot rebuilds broker state from a snapshot.
// It restores exchanges, queues, bindings, vhosts, and cluster nodes.
func (s *Server) RestoreFromSnapshot(data *SnapshotData) error {
	if data == nil {
		return nil
	}
	if data.Version != snapshotVersion {
		return fmt.Errorf(
			"unsupported snapshot version %q (expected %q)",
			data.Version, snapshotVersion,
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second)
	defer cancel()

	// Restore exchanges.
	for _, ex := range data.Exchanges {
		var et ExchangeType
		switch ex.Type {
		case "direct":
			et = ExchangeDirect
		case "fanout":
			et = ExchangeFanout
		case "topic":
			et = ExchangeTopic
		case "headers":
			et = ExchangeHeaders
		default:
			et = ExchangeDirect
		}
		_, err := s.exchanges.Declare(
			ex.Name, et, ex.Durable,
			ex.AutoDelete, ex.Internal, ex.Args,
		)
		if err != nil {
			log.Printf("restore exchange %q: %v", ex.Name, err)
		} else if s.metaStore != nil {
			_ = s.metaStore.SaveExchange(ctx, store.ExchangeMeta{
				Name:       ex.Name,
				Type:       ex.Type,
				Durable:    ex.Durable,
				AutoDelete: ex.AutoDelete,
				Internal:   ex.Internal,
				Args:       ex.Args,
			})
		}
	}

	// Restore queues.
	for _, q := range data.Queues {
		_, err := s.queues.Declare(
			q.Name, q.Durable, q.Exclusive,
			q.AutoDelete, q.Args, nil,
		)
		if err != nil {
			log.Printf("restore queue %q: %v", q.Name, err)
		} else if s.metaStore != nil {
			_ = s.metaStore.SaveQueue(ctx, store.QueueMeta{
				Name:       q.Name,
				Durable:    q.Durable,
				Exclusive:  q.Exclusive,
				AutoDelete: q.AutoDelete,
				Args:       q.Args,
			})
		}
	}

	// Restore bindings.
	for _, b := range data.Bindings {
		_, err := s.bindings.Bind(
			b.Exchange, b.Queue, b.RoutingKey, b.Args,
		)
		if err != nil {
			log.Printf(
				"restore binding %s-%s-%s: %v",
				b.Exchange, b.Queue, b.RoutingKey, err,
			)
		} else if s.metaStore != nil {
			_ = s.metaStore.SaveBinding(ctx, store.BindingMeta{
				Exchange:   b.Exchange,
				Queue:      b.Queue,
				RoutingKey: b.RoutingKey,
				Args:       b.Args,
			})
		}
	}

	// Restore VHosts (skip default "/" which is always present).
	for _, vh := range data.VHosts {
		if vh.Name == "/" {
			continue
		}
		s.vhosts.Create(vh.Name, vh.Description)
	}

	// Restore cluster nodes.
	if s.cluster != nil {
		for _, n := range data.ClusterNodes {
			if n.ID == s.cluster.LocalID() {
				continue
			}
			s.cluster.Join(n.ID, n.Addr)
		}
	}

	log.Printf(
		"restored from snapshot: %d exchanges, %d queues, %d bindings, %d vhosts, %d cluster nodes",
		len(data.Exchanges), len(data.Queues),
		len(data.Bindings), len(data.VHosts),
		len(data.ClusterNodes),
	)
	return nil
}
