// end_to_end_bench_test.go — full publish → route → store → deliver
// → consume → ack latency distribution.
package bench

import (
	"sort"
	"testing"
	"time"

	"github.com/qdongxu/gomq/internal/server"
)

// BenchmarkEndToEnd measures the complete message lifecycle latency.
// It pre-fills the queue, then for each iteration records the time
// taken to publish, route, store, dequeue, deliver and ack one
// message.
func BenchmarkEndToEnd(b *testing.B) {
	srv := benchServer()
	setupDirect(srv, "ex.direct", "q1", "rk")
	pub := srv.Publisher()
	store := srv.MessageStore()
	cm := srv.ConsumerManager()
	tracker := srv.DeliveryTracker()

	auth := server.NewMemoryAuthenticator()
	conn := server.NewConnection(nil, auth, nil)
	ch, _ := server.NewChannelManager(10).Create(1, conn)
	ch.Open()
	_, _ = cm.Subscribe("c1", "q1", ch, false, false, nil)

	deliverer := server.NewDeliverer(cm, store, tracker)

	msg := benchMessage(256)

	// Warm-up: 1000 iterations to stabilise caches / JIT.
	for i := 0; i < 1000; i++ {
		msg.SetRoutingMeta("ex.direct", "rk")
		_, _ = pub.Publish("ex.direct", "rk", msg, 1)
		m, _ := store.Dequeue("q1")
		_ = deliverer.Deliver(m, "q1", 1)
		_ = tracker.Ack(uint64(i), 1)
	}

	// Pre-fill queue for the actual benchmark.
	for i := 0; i < b.N+100; i++ {
		msg.SetRoutingMeta("ex.direct", "rk")
		_, _ = pub.Publish("ex.direct", "rk", msg, 1)
	}

	latencies := make([]time.Duration, 0, b.N)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()

		msg.SetRoutingMeta("ex.direct", "rk")
		_, err := pub.Publish("ex.direct", "rk", msg, 1)
		if err != nil {
			b.Fatalf("publish: %v", err)
		}

		m, ok := store.Dequeue("q1")
		if !ok {
			b.Fatal("queue empty")
		}
		_ = deliverer.Deliver(m, "q1", 1)
		_ = tracker.Ack(uint64(i), 1)

		latencies = append(latencies, time.Since(start))
	}

	// Compute and report latency percentiles.
	reportPercentiles(b, latencies)
	b.SetBytes(256)
}

// BenchmarkEndToEndParallel runs the same full lifecycle across
// GOMAXPROCS goroutines.
func BenchmarkEndToEndParallel(b *testing.B) {
	srv := benchServer()
	setupDirect(srv, "ex.direct", "q1", "rk")
	pub := srv.Publisher()
	store := srv.MessageStore()
	cm := srv.ConsumerManager()
	tracker := srv.DeliveryTracker()

	auth := server.NewMemoryAuthenticator()
	conn := server.NewConnection(nil, auth, nil)
	ch, _ := server.NewChannelManager(10).Create(1, conn)
	ch.Open()
	_, _ = cm.Subscribe("c1", "q1", ch, false, false, nil)

	deliverer := server.NewDeliverer(cm, store, tracker)
	msg := benchMessage(256)

	// Pre-fill.
	for i := 0; i < b.N+1000; i++ {
		msg.SetRoutingMeta("ex.direct", "rk")
		_, _ = pub.Publish("ex.direct", "rk", msg, 1)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(256)
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			msg.SetRoutingMeta("ex.direct", "rk")
			_, err := pub.Publish("ex.direct", "rk", msg, 1)
			if err != nil {
				b.Fatalf("publish: %v", err)
			}
			m, ok := store.Dequeue("q1")
			if !ok {
				b.Fatal("queue empty")
			}
			_ = deliverer.Deliver(m, "q1", 1)
			_ = tracker.Ack(i, 1)
			i++
		}
	})
}

// reportPercentiles sorts latencies and logs p50 / p95 / p99 via
// b.Log so they appear in benchmark output when -benchmem is used.
func reportPercentiles(b *testing.B, latencies []time.Duration) {
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	n := len(latencies)
	if n == 0 {
		return
	}
	p50 := latencies[n*50/100]
	p95 := latencies[n*95/100]
	p99 := latencies[n*99/100]

	b.ReportMetric(float64(p50.Nanoseconds())/1e3, "us/p50")
	b.ReportMetric(float64(p95.Nanoseconds())/1e3, "us/p95")
	b.ReportMetric(float64(p99.Nanoseconds())/1e3, "us/p99")
}
