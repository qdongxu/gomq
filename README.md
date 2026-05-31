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

# Build with version injection
make build

# Verify version
./bin/gomqd -version

# Run tests
make test

# Run benchmarks
make bench

# Format + lint + test (CI gate)
make check

# Build Docker image
make docker

# Cross-compile release binaries
make release

# Run with default config
./bin/gomqd -config configs/gomq.default.toml
```

The server listens on **5672** (AMQP) and **15672** (Web UI) by default.

## Installation

### From Source

```bash
go install github.com/qdongxu/gomq/cmd/gomqd@latest
```

### Binary Release

Download pre-built binaries from the [Releases](https://github.com/qdongxu/gomq/releases) page.

## Build & Release

### Make Targets

| Target | Purpose |
|--------|---------|
| `make build` | Compile `bin/gomqd` with version injection |
| `make test` | Run unit + integration tests |
| `make bench` | Run benchmark suite (`tests/bench/`) |
| `make lint` | Run `golangci-lint` with project config |
| `make fmt` | Auto-format with `gofmt` + `goimports` |
| `make docker` | Build multi-stage Docker image (`< 20 MB`) |
| `make release` | Cross-compile for linux/darwin × amd64/arm64 |
| `make clean` | Remove `bin/` and `dist/` |
| `make check` | CI gate: `fmt` + `lint` + `test` |

### Docker

```bash
make docker
# → qdongxu/gomqd:latest

docker run -p 5672:5672 -p 15672:15672 \
  -v $(pwd)/configs/gomq.default.toml:/etc/gomq/gomq.default.toml \
  qdongxu/gomqd:latest
```

The image uses a two-stage build:
1. `golang:1.25-alpine` compiles a static binary (`CGO_ENABLED=0`)
2. `gcr.io/distroless/static:nonroot` runs the final image (no shell, minimal attack surface)

### Cross-Compilation

```bash
make release
# Generates:
#   dist/gomqd-linux-amd64
#   dist/gomqd-linux-arm64
#   dist/gomqd-darwin-amd64
#   dist/gomqd-darwin-arm64
```

### Version Injection

`make build` injects the Git tag and build time via ldflags:

```bash
./bin/gomqd -version
# gomqd v0.1.0-54-g<hash> (built 2026-05-28T02:55:00Z)
```

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

### Quorum Queue & Raft Network Layer

gomq uses a simplified Raft consensus algorithm for multi-node Quorum Queue replication:

- **Raft State Machine**: Leader election, log replication, heartbeat mechanism
- **Transport Layer**: In-memory transport (testing) and HTTP/JSON transport (production)
- **Failover**: Automatic re-election when the leader fails
- **Integration Tests**: 3-node local cluster verifying leader election, log replication, and failover

Implementation is in `internal/cluster/`:

| File | Responsibility |
|------|----------------|
| `raft.go` | Core Raft state machine (Term, Log, CommitIndex) |
| `raft_transport.go` | Transport interface + in-memory transport |
| `raft_rpc.go` | HTTP/JSON transport implementation |
| `raft_node.go` | Multi-node extension (Run loop, election, heartbeat) |
| `raft_integration_test.go` | 3-node integration test |

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

### SASL Authentication

gomq supports three SASL mechanisms during AMQP connection startup:

| Mechanism | Description | Requirements |
|-----------|-------------|--------------|
| `PLAIN` | Username/password (default) | None |
| `AMQPLAIN` | RabbitMQ-compatible username/password | None |
| `EXTERNAL` | TLS client certificate CN as identity | TLS with mutual auth |

The server advertises all available mechanisms in `Connection.Start`.  The
client selects one in `Connection.Start-Ok`.

**EXTERNAL** extracts the CommonName from the peer's TLS certificate:

```toml
[tls]
enabled       = true
cert_file     = "/etc/gomq/server.crt"
key_file      = "/etc/gomq/server.key"
ca_file       = "/etc/gomq/ca.crt"
verify_client = true   # required for EXTERNAL
```

Clients presenting a valid certificate with CN `alice` will be
authenticated as user `alice` without sending a password.

### Memory Optimization

```toml
[memory]
# Minimum payload size (bytes) to trigger zlib compression.
# 0 = disabled.
compression_threshold = 1024

# Maximum messages per queue kept in memory before paging to disk.
# 0 = disabled.
max_in_memory_messages = 10000

# Directory for on-disk page files.
page_dir = "/var/lib/gomq/pages"
```

When **compression** is enabled, messages with payloads larger than the
threshold are transparently compressed on enqueue and decompressed on
dequeue.  When **paging** is enabled, older messages are flushed to
page files when a queue exceeds the in-memory limit; they can be
reloaded on demand.

### Rate Limiting & Backpressure

```toml
[limits]
# Maximum new connections per second.  0 = unlimited.
max_connections_per_second = 100

# Memory-usage percentage that triggers backpressure.
# 0 = disabled.
memory_threshold_percent = 80

# Master switch for backpressure control.
backpressure_enabled = true
```

The **token-bucket rate limiter** rejects connections when the burst
is exhausted.  **Backpressure** monitors heap memory; when it exceeds
the threshold, new connections are refused and publishers may receive
`Channel.Flow` (pause).

### Web UI

```toml
[web]
enabled = true
listen = "0.0.0.0:15672"
path_prefix = "/"
```

The management UI is built with **htmx** and provides real-time views of connections, channels, exchanges, queues, bindings, and admin controls.

### Management Endpoints (Health & Readiness)

```toml
[management]
# Enable /api/health and /api/ready endpoints.
health_enabled = true

# Enable /debug/pprof/* (only when log.level = "debug").
pprof_enabled = false

# Dedicated bind address; leave empty to reuse the web UI port.
bind_address = ""
```

| Endpoint | Method | Purpose | Response |
|----------|--------|---------|----------|
| `/api/health` | GET | Node health (up/down), version, uptime, component checks | `200 OK` + JSON |
| `/api/ready` | GET | Readiness probe (listener + store status) | `200 OK` or `503 Service Unavailable` + JSON |
| `/debug/pprof/` | GET | Runtime profiles (heap, CPU, goroutine, mutex) | `200 OK` (debug only) |

The readiness endpoint returns `503` when the AMQP listener is not active or the persistence store (etcd) is unreachable.

### Hot Reload

```toml
[management]
# Enable config file watching for hot reload.
health_enabled = true
```

gomq watches the configuration file for changes and applies reloadable settings without restarting the process. Send `SIGHUP` to force a reload:

```bash
kill -HUP $(pgrep gomqd)
```

| Reloadable | Non-reloadable (requires restart) |
|-----------|----------------------------------|
| Log level | Network listeners |
| TLS certificates | etcd endpoints |
| ACL rules | Cluster node ID |
| Rate limits | Web UI / metrics ports |
| Backpressure thresholds | |
| Memory settings | |

When a non-reloadable key is changed, a warning is logged and the key is ignored until the next restart.

## Project Structure

| Path | Description |
|------|-------------|
| `cmd/gomqd/` | Server entry point |
| `internal/server/` | AMQP connection, channel, exchange, queue core |
| `internal/auth/` | ACL rule engine **+ SASL mechanisms** |
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
| **Quorum Queue — Multi-node Raft network layer** | ✅ |
| Exchange-to-Exchange Binding | ✅ |
| Cluster node discovery via etcd | ✅ |
| TLS support (AMQP over TLS + mTLS) | ✅ |
| Prometheus metrics export | ✅ |
| Memory pool & batching (performance) | ✅ |
| Channel.Recover and edge methods | ✅ |
| ACL (Access Control List) — vhost-level permissions | ✅ |
| **SASL authentication (PLAIN / AMQPLAIN / EXTERNAL)** | ✅ |
| **Memory compression & paging (zlib, disk overflow)** | ✅ |
| **Rate limiting & backpressure** | ✅ |
| **Connection.CloseOk / Channel.CloseOk handlers** | ✅ |
| **Basic.Reject (requeue + no-requeue + DLX)** | ✅ |
| Mirrored Queue (HA Queue) | ✅ |
| Federation / Shovel | ✅ |
| **Config hot reload** | ✅ |
| **Health check & readiness probe** | ✅ |
| **pprof runtime profiling (debug mode)** | ✅ |
| **Benchmark suite** | ✅ |
| **Compatibility test suite** | ⚠️ | Infrastructure ready; blocked by protocol handshake issue with amqp091-go client |

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
