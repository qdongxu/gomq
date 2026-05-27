# gomq Benchmark Suite

This directory contains performance benchmarks for the core message
broker paths: publish, consume, exchange routing, and end-to-end
latency.

## Running Benchmarks

```bash
cd gomq

# Run all benchmarks (default 1s per benchmark)
go test -bench=. ./tests/bench/

# Run with longer benchtime for more stable results
go test -bench=. -benchtime=2s ./tests/bench/

# Run a specific benchmark
go test -bench=BenchmarkPublish ./tests/bench/

# Run with memory allocation stats
go test -bench=. -benchmem ./tests/bench/

# Run with CPU profile
go test -bench=BenchmarkEndToEnd -cpuprofile=cpu.prof ./tests/bench/
go tool pprof cpu.prof
```

## Benchmarks

### Publish Throughput

| Benchmark | What it measures |
|-----------|-----------------|
| `BenchmarkPublish/64B` | Single-connection publish throughput with 64-byte messages |
| `BenchmarkPublish/1KB` | Publish throughput with 1 KB messages |
| `BenchmarkPublish/16KB` | Publish throughput with 16 KB messages |
| `BenchmarkPublishParallel` | Contended publish from GOMAXPROCS goroutines |
| `BenchmarkPublishMultipleExchanges` | Publish spread across 10 independent exchanges |

**Metrics**: `ns/op`, `msg/s` (derived), `B/op` (allocations)

### Consume Throughput

| Benchmark | What it measures |
|-----------|-----------------|
| `BenchmarkConsume` | Dequeue + deliver + ack for a single consumer |
| `BenchmarkConsumeAutoAck` | Same but with auto-ack (no explicit tracking) |
| `BenchmarkConsumeMultipleConsumers` | 4 consumers sharing one queue |

Queues are pre-filled before `b.ResetTimer()` so the benchmark measures
only the consume path.

### Routing Comparison

| Benchmark | Exchange type | What it measures |
|-----------|---------------|-----------------|
| `BenchmarkRoutingDirect` | direct | Exact string match, single queue |
| `BenchmarkRoutingFanout` | fanout | Broadcast to 4 queues |
| `BenchmarkRoutingTopic` | topic | `*` and `#` wildcard matching |
| `BenchmarkRoutingHeaders` | headers | Key-value header matching |

**Goal**: identify which exchange type adds the most routing overhead.

### End-to-End Latency

| Benchmark | What it measures |
|-----------|-----------------|
| `BenchmarkEndToEnd` | Full chain: publish → route → store → dequeue → deliver → ack |
| `BenchmarkEndToEndParallel` | Same chain from multiple goroutines |

**Metrics**:
- `ns/op` — average latency per message
- `us/p50` — 50th percentile latency (microseconds)
- `us/p95` — 95th percentile latency
- `us/p99` — 99th percentile latency

A 1000-iteration warm-up runs before timing to stabilise CPU caches.

## Interpreting Results

### Converting ns/op to msg/s

```
msg/s = 1e9 / ns/op
```

Example: `1056 ns/op` → `947,368 msg/s`.

### Memory allocations

`allocs/op` shows heap allocations per operation. Zero-allocation paths
are ideal; any non-zero value indicates a target for optimisation.

### Comparing to RabbitMQ

RabbitMQ community benchmarks typically report:
- **Publish**: 100k–500k msg/s (single node, no persistence)
- **Consume**: 100k–300k msg/s (auto-ack, in-memory)
- **End-to-end**: 50k–150k msg/s

gomq targets parity with RabbitMQ single-node in-memory performance.
Persistence (etcd) and clustering add overhead not captured in these
pure-memory benchmarks.

## Environment Recommendations

For reproducible results:

```bash
# Pin to one CPU to avoid scheduler noise
GOMAXPROCS=1 go test -bench=. ./tests/bench/

# Or use all cores for parallel benchmarks
go test -bench=. -benchtime=5s ./tests/bench/

# Disable CPU frequency scaling (Linux)
sudo cpupower frequency-set -g performance

# Run multiple times and take the median
for i in {1..5}; do
  go test -bench=BenchmarkPublish -benchtime=2s ./tests/bench/
done
```

## Adding New Benchmarks

1. Create `tests/bench/<name>_bench_test.go`
2. Use `benchServer()` and helper functions from `setup.go`
3. Call `b.ResetTimer()` after setup
4. Call `b.ReportAllocs()` to capture allocations
5. Use `b.SetBytes(size)` so throughput can be derived

Example:

```go
func BenchmarkMyFeature(b *testing.B) {
    srv := benchServer()
    setupDirect(srv, "ex", "q", "rk")
    pub := srv.Publisher()
    msg := benchMessage(256)

    b.ResetTimer()
    b.ReportAllocs()
    for i := 0; i < b.N; i++ {
        _, _ = pub.Publish("ex", "rk", msg, 1)
    }
    b.SetBytes(256)
}
```
