# gomq

A Go-native message broker compatible with AMQP 0-9-1 clients.
The goal is to reimplement RabbitMQ server functionality in Go,
with embedded etcd for persistence and htmx for the web management UI.

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

WIP — documentation and features will be updated as code evolves.

## License

MIT
