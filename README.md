# gomq

A Go-native message broker compatible with AMQP 0-9-1 clients.
The goal is to reimplement RabbitMQ server functionality in Go,
with embedded etcd for persistence and htmx for the web management UI.

## Configuration

gomq uses TOML configuration files. See `configs/gomq.default.toml`
for all available options. Environment variables override file values
using the prefix `GOMQ_` (e.g. `GOMQ_NETWORK_HEARTBEAT=30`).

## Quick Start

```bash
# Build
make build

# Run
./bin/gomqd -config configs/gomq.default.toml

# Test
make test
```

## Project Structure

- `cmd/gomqd/` — server entry point
- `internal/server/` — AMQP connection, channel, exchange, queue core
- `internal/store/` — etcd persistence wrapper
- `internal/config/` — TOML configuration parser
- `internal/web/` — htmx management UI
- `internal/cluster/` — clustering and node management
- `pkg/protocol/amqp091/` — AMQP 0-9-1 wire protocol codec
- `test/integration/` — integration tests

## Status

| Feature | Status |
|---------|--------|
| AMQP 0-9-1 wire protocol | ✅ |
| Connection handshake & heartbeat | ✅ |
| Channel multiplexing | ✅ |
| Exchange declare / delete (direct, fanout) | ✅ |
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
| Message in-memory store | ✅ |
| Routing & delivery | ✅ |
| etcd persistence (Store interface + memory backend) | ✅ |
| etcd persistence (etcd backend) | WIP |
| Load persisted state on startup | ✅ |
| Web management UI (htmx) | ✅ |
| Clustering | ✅ |

WIP — documentation and features will be updated as code evolves.

## License

MIT
