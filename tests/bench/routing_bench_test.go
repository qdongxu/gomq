// routing_bench_test.go — exchange routing performance comparison.
package bench

import (
	"fmt"
	"testing"

	"github.com/qdongxu/gomq/internal/server"
)

// BenchmarkRoutingDirect measures the cheapest routing path:
// exact string match on a direct exchange with one queue.
func BenchmarkRoutingDirect(b *testing.B) {
	srv := benchServer()
	setupDirect(srv, "ex.direct", "q1", "rk")
	pub := srv.Publisher()

	msg := benchMessage(256)
	msg.SetRoutingMeta("ex.direct", "rk")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := pub.Publish("ex.direct", "rk", msg, 1)
		if err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
	b.SetBytes(256)
}

// BenchmarkRoutingFanout measures broadcast routing to 4 queues.
func BenchmarkRoutingFanout(b *testing.B) {
	srv := benchServer()
	queues := []string{"q0", "q1", "q2", "q3"}
	setupFanout(srv, "ex.fanout", queues)
	pub := srv.Publisher()

	msg := benchMessage(256)
	msg.SetRoutingMeta("ex.fanout", "")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := pub.Publish("ex.fanout", "", msg, 1)
		if err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
	b.SetBytes(256)
}

// BenchmarkRoutingTopic measures wildcard routing overhead with a
// mix of * and # patterns.
func BenchmarkRoutingTopic(b *testing.B) {
	srv := benchServer()
	bindings := map[string]string{
		"q1": "stock.usd.nyse",
		"q2": "stock.*.nyse",
		"q3": "stock.#",
		"q4": "#",
	}
	setupTopic(srv, "ex.topic", bindings)
	pub := srv.Publisher()

	msg := benchMessage(256)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rk := fmt.Sprintf("stock.usd.nyse.%d", i%100)
		msg.SetRoutingMeta("ex.topic", rk)
		_, err := pub.Publish("ex.topic", rk, msg, 1)
		if err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
	b.SetBytes(256)
}

// BenchmarkRoutingHeaders measures key-value matching overhead on a
// headers exchange with three match criteria.
func BenchmarkRoutingHeaders(b *testing.B) {
	srv := benchServer()
	bindingHeaders := map[string]interface{}{
		"x-match":    "all",
		"format":     "json",
		"priority":   "high",
		"department": "engineering",
	}
	setupHeaders(srv, "ex.headers", "q1", bindingHeaders)
	pub := srv.Publisher()

	msgHeaders := map[string]interface{}{
		"format":     "json",
		"priority":   "high",
		"department": "engineering",
	}
	msg := server.NewMessage(benchPayload(256), server.Properties{Headers: msgHeaders})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msg.SetRoutingMeta("ex.headers", "")
		_, err := pub.Publish("ex.headers", "", msg, 1)
		if err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
	b.SetBytes(256)
}

// benchPayload returns a deterministic payload of the requested size.
func benchPayload(size int) []byte {
	p := make([]byte, size)
	for i := range p {
		p[i] = byte(i % 256)
	}
	return p
}
