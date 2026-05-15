//go:build etcd

// etcd.go provides an etcd-backed Store implementation.
// This file is only compiled when the "etcd" build tag is present.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdStore persists broker metadata in etcd.
type EtcdStore struct {
	client *clientv3.Client
	prefix string
}

// NewEtcdStore creates a store using the given etcd client and
// key prefix (e.g. "/gomq").
func NewEtcdStore(
	client *clientv3.Client,
	prefix string,
) *EtcdStore {
	return &EtcdStore{
		client: client,
		prefix: prefix,
	}
}

func (s *EtcdStore) ctxWithTimeout(
	parent context.Context,
) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, DefaultTimeout)
}

// SaveQueue persists queue metadata.
func (s *EtcdStore) SaveQueue(
	ctx context.Context,
	meta QueueMeta,
) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	c, cancel := s.ctxWithTimeout(ctx)
	defer cancel()
	key := path.Join(s.prefix, "queues", meta.Name)
	_, err = s.client.Put(c, key, string(data))
	return err
}

// DeleteQueue removes a queue from etcd.
func (s *EtcdStore) DeleteQueue(
	ctx context.Context,
	name string,
) error {
	c, cancel := s.ctxWithTimeout(ctx)
	defer cancel()
	key := path.Join(s.prefix, "queues", name)
	_, err := s.client.Delete(c, key)
	return err
}

// LoadQueues returns all persisted queues.
func (s *EtcdStore) LoadQueues(
	ctx context.Context,
) ([]QueueMeta, error) {
	c, cancel := s.ctxWithTimeout(ctx)
	defer cancel()
	key := path.Join(s.prefix, "queues")
	resp, err := s.client.Get(c, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]QueueMeta, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var meta QueueMeta
		if err := json.Unmarshal(kv.Value, &meta); err != nil {
			return nil, fmt.Errorf("unmarshal queue: %w", err)
		}
		out = append(out, meta)
	}
	return out, nil
}

// SaveExchange persists exchange metadata.
func (s *EtcdStore) SaveExchange(
	ctx context.Context,
	meta ExchangeMeta,
) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	c, cancel := s.ctxWithTimeout(ctx)
	defer cancel()
	key := path.Join(s.prefix, "exchanges", meta.Name)
	_, err = s.client.Put(c, key, string(data))
	return err
}

// DeleteExchange removes an exchange from etcd.
func (s *EtcdStore) DeleteExchange(
	ctx context.Context,
	name string,
) error {
	c, cancel := s.ctxWithTimeout(ctx)
	defer cancel()
	key := path.Join(s.prefix, "exchanges", name)
	_, err := s.client.Delete(c, key)
	return err
}

// LoadExchanges returns all persisted exchanges.
func (s *EtcdStore) LoadExchanges(
	ctx context.Context,
) ([]ExchangeMeta, error) {
	c, cancel := s.ctxWithTimeout(ctx)
	defer cancel()
	key := path.Join(s.prefix, "exchanges")
	resp, err := s.client.Get(c, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]ExchangeMeta, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var meta ExchangeMeta
		if err := json.Unmarshal(kv.Value, &meta); err != nil {
			return nil, fmt.Errorf("unmarshal exchange: %w", err)
		}
		out = append(out, meta)
	}
	return out, nil
}

// SaveBinding persists a binding.
func (s *EtcdStore) SaveBinding(
	ctx context.Context,
	meta BindingMeta,
) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	c, cancel := s.ctxWithTimeout(ctx)
	defer cancel()
	key := path.Join(
		s.prefix, "bindings",
		fmt.Sprintf("%s|%s|%s", meta.Exchange, meta.Queue,
			meta.RoutingKey),
	)
	_, err = s.client.Put(c, key, string(data))
	return err
}

// DeleteBinding removes a binding from etcd.
func (s *EtcdStore) DeleteBinding(
	ctx context.Context,
	exchange, queue, routingKey string,
) error {
	c, cancel := s.ctxWithTimeout(ctx)
	defer cancel()
	key := path.Join(
		s.prefix, "bindings",
		fmt.Sprintf("%s|%s|%s", exchange, queue, routingKey),
	)
	_, err := s.client.Delete(c, key)
	return err
}

// LoadBindings returns all persisted bindings.
func (s *EtcdStore) LoadBindings(
	ctx context.Context,
) ([]BindingMeta, error) {
	c, cancel := s.ctxWithTimeout(ctx)
	defer cancel()
	key := path.Join(s.prefix, "bindings")
	resp, err := s.client.Get(c, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make([]BindingMeta, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var meta BindingMeta
		if err := json.Unmarshal(kv.Value, &meta); err != nil {
			return nil, fmt.Errorf("unmarshal binding: %w", err)
		}
		out = append(out, meta)
	}
	return out, nil
}

// Close closes the underlying etcd client.
func (s *EtcdStore) Close() error {
	return s.client.Close()
}
