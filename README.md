# gomq

A Go-native message broker compatible with AMQP 0-9-1 clients.
The goal is to reimplement RabbitMQ server functionality in Go,
with embedded etcd for persistence and htmx for the web management UI.

## Requirements

- **Go** 1.25 or later
- **etcd** 3.5+ (optional, for cluster persistence)
- **Make** (optional, for convenience targets)

## Quick Start

```bash
# Clone
git clone https://github.com/qdongxu/gomq.git
cd gomq

# Build
make build

# Run with default config
./bin/gomqd -config configs/gomq.default.toml

# Run tests
make test
```

The server listens on **5672** (AMQP) and **15672** (Web UI) by default.

## Installation

### From Source

```bash
go install github.com/qdongxu/gomq/cmd/gomqd@latest
```

### Binary Release

Download pre-built binaries from the [Releases](https://github.com/qdongxu/gomq/releases) page.

## Configuration

gomq uses **TOML** configuration files. See `configs/gomq.default.toml` for a complete example.

### Environment Variable Override

Any config value can be overridden via environment variables using the `GOMQ_` prefix and underscore-delimited path:

```bash
GOMQ_NETWORK_HEARTBEAT=30
GOMQ_TLS_ENABLED=true
GOMQ_TLS_CERT_FILE=/etc/gomq/server.crt
```

### Minimal Config

```toml
[network]
listeners = ["0.0.0.0:5672"]
heartbeat = 60

[log]
level = "info"
output = "stdout"
```

### TLS Config

```toml
[tls]
enabled = true
cert_file = "/etc/gomq/server.crt"
key_file  = "/etc/gomq/server.key"

# Optional: mutual TLS
ca_file       = "/etc/gomq/ca.crt"
verify_client = true
```

When TLS is enabled, gomq listens on **5671** (or the second entry in `network.listeners`).

### Cluster Config

```toml
[cluster]
node_id = "node-1"
discovery = "etcd"
etcd_endpoints = ["http://localhost:2379"]
```

Static discovery (no etcd):

```toml
[cluster]
node_id = "node-1"
discovery = "static"
nodes = ["node-2@192.168.1.10:5672", "node-3@192.168.1.11:5672"]
```

### Prometheus Metrics

```toml
[metrics]
enabled = true
listen = "0.0.0.0:15692"
```

Metrics are exposed at `http://<listen>/metrics` in Prometheus format.

### ACL (Access Control)

```toml
[[acl.rules]]
user = "admin"
vhost = "/"
resource_type = "*"
resource_name = "*"
permission = "*"
allow = true

[[acl.rules]]
user = "guest"
vhost = "/"
resource_type = "exchange"
resource_name = "amq.*"
permission = "write"
allow = true

[[acl.rules]]
user = "*"
vhost = "*"
resource_type = "*"
resource_name = "*"
permission = "*"
allow = false
```

Rules are evaluated **in order**; the first match wins. If no rule matches, access is denied.

| Permission | Maps to AMQP Operations |
|------------|------------------------|
| `configure` | Exchange.Declare/Delete/Bind/Unbind, Queue.Declare/Bind/Delete/Purge/Unbind |
| `write` | Basic.Publish |
| `read` | Basic.Consume, Basic.Get |

### Web UI

```toml
[web]
enabled = true
listen = "0.0.0.0:15672"
path_prefix = "/"
```

The management UI is built with **htmx** and provides real-time views of connections, channels, exchanges, queues, bindings, and admin controls.

## Project Structure

| Path | Description |
|------|-------------|
| `cmd/gomqd/` | Server entry point |
| `internal/server/` | AMQP connection, channel, exchange, queue core |
| `internal/auth/` | ACL rule engine |
| `internal/store/` | etcd and memory persistence backends |
| `internal/config/` | TOML configuration parser |
| `internal/web/` | htmx management UI |
| `internal/cluster/` | Clustering, node discovery, Raft quorum queues |
| `internal/metrics/` | Prometheus metrics collector |
| `internal/queue/` | Queue implementations (memory, quorum) |
| `pkg/protocol/amqp091/` | AMQP 0-9-1 wire protocol codec |
| `test/integration/` | Integration tests |

## Feature Status

| Feature | Status |
|---------|--------|
| AMQP 0-9-1 wire protocol | ✅ |
| Connection handshake & heartbeat | ✅ |
| Channel multiplexing | ✅ |
| Exchange declare / delete (direct, fanout, topic, headers) | ✅ |
| Queue declare / delete / bind / unbind / purge | ✅ |
| Basic.Publish / Basic.Get / Basic.Consume | ✅ |
| Basic.Ack / Basic.Nack / Basic.Reject | ✅ |
| Basic.Qos (per-channel & global prefetch) | ✅ |
| Basic.Recover | ✅ |
| Connection.Close / Channel.Close | ✅ |
| Channel.Flow (client-to-server) | ✅ |
| Basic.Return (mandatory publish) | ✅ |
| Publisher Confirm (Confirm.Select + Basic.Ack/Nack) | ✅ |
| Transaction support (Tx.Select / Tx.Commit / Tx.Rollback) | ✅ |
| Dead letter exchange (DLX) | ✅ |
| Message TTL (per-message & per-queue) | ✅ |
| Priority queues | ✅ |
| Message in-memory store | ✅ |
| Routing & delivery | ✅ |
| etcd persistence (Store interface + memory backend) | ✅ |
| etcd persistence (etcd backend) | ✅ |
| Load persisted state on startup | ✅ |
| Web management UI (htmx framework) | ✅ |
| Web management UI — Overview page | ✅ |
| Web management UI — Connections page | ✅ |
| Web management UI — Channels page | ✅ |
| Web management UI — Exchanges page | ✅ |
| Web management UI — Queues page | ✅ |
| Web management UI — Bindings page | ✅ |
| Web management UI — Admin page | ✅ |
| Quorum Queue (Raft-based replication) | ✅ |
| Exchange-to-Exchange Binding | ✅ |
| Cluster node discovery via etcd | ✅ |
| TLS support (AMQP over TLS + mTLS) | ✅ |
| Prometheus metrics export | ✅ |
| Memory pool & batching (performance) | ✅ |
| Channel.Recover and edge methods | ✅ |
| ACL (Access Control List) — vhost-level permissions | ✅ |
| Mirrored Queue (HA Queue) | ✅ |
| Plugin System | ✅ |
| Federation / Shovel | ✅ |

## Development

```bash
# Format code
make fmt

# Run linter
make lint

# Clean build artifacts
make clean
```

### Running Integration Tests

```bash
# Start a local etcd instance first, then:
go test ./test/integration/...
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/your-feature`
3. Commit your changes (follow [Conventional Commits](https://www.conventionalcommits.org/))
4. Push to your fork and open a Pull Request

## License

MIT
