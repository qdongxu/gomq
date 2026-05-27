// publish_bench_test.go — publish throughput benchmarks.
package bench

import (
	"fmt"
	"testing"
)

// BenchmarkPublish measures raw publish throughput through a direct
// exchange with a single bound queue.  Three payload sizes are tested
// to show how message size affects throughput.
func BenchmarkPublish(b *testing.B) {
	sizes := []int{64, 1024, 16 * 1024}
	labels := []string{"64B", "1KB", "16KB"}

	for i, size := range sizes {
		b.Run(labels[i], func(b *testing.B) {
			srv := benchServer()
			setupDirect(srv, "ex.direct", "q1", "rk")
			pub := srv.Publisher()

			msg := benchMessage(size)
			msg.SetRoutingMeta("ex.direct", "rk")

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Publish clones the message internally; we reuse the
				// same msg pointer to keep the benchmark focused on the
				// routing / store path.
				_, err := pub.Publish("ex.direct", "rk", msg, 1)
				if err != nil {
					b.Fatalf("publish: %v", err)
				}
			}
			b.SetBytes(int64(size))
		})
	}
}

// BenchmarkPublishParallel measures publish throughput with GOMAXPROCS
// goroutines contending on the same exchange and queue.
func BenchmarkPublishParallel(b *testing.B) {
	srv := benchServer()
	setupDirect(srv, "ex.direct", "q1", "rk")
	pub := srv.Publisher()

	msg := benchMessage(256)
	msg.SetRoutingMeta("ex.direct", "rk")

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(256)
	b.RunParallel(func(pb *testing.PB) {
		chID := uint16(1)
		for pb.Next() {
			_, err := pub.Publish("ex.direct", "rk", msg, chID)
			if err != nil {
				b.Fatalf("publish: %v", err)
			}
		}
	})
}

// BenchmarkPublishMultipleExchanges measures throughput when messages
// are spread across 10 independent direct exchanges.
func BenchmarkPublishMultipleExchanges(b *testing.B) {
	srv := benchServer()
	const nEx = 10
	for i := 0; i < nEx; i++ {
		setupDirect(srv,
			fmt.Sprintf("ex.%d", i),
			fmt.Sprintf("q.%d", i),
			"rk")
	}
	pub := srv.Publisher()

	msg := benchMessage(256)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		exIdx := i % nEx
		ex := fmt.Sprintf("ex.%d", exIdx)
		msg.SetRoutingMeta(ex, "rk")
		_, err := pub.Publish(ex, "rk", msg, 1)
		if err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
	b.SetBytes(256)
}
