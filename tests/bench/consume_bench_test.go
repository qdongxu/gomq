// consume_bench_test.go — consume (dequeue + deliver + ack) throughput.
package bench

import (
	"fmt"
	"testing"

	"github.com/qdongxu/gomq/internal/server"
)

// BenchmarkConsume measures the throughput of a single consumer
// doing dequeue → deliver → ack in a tight loop.  The queue is
// pre-filled so the benchmark measures only the consume path.
func BenchmarkConsume(b *testing.B) {
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

	// Pre-fill the queue so the benchmark only measures consume.
	msg := benchMessage(256)
	for i := 0; i < b.N+1000; i++ {
		msg.SetRoutingMeta("ex.direct", "rk")
		_, _ = pub.Publish("ex.direct", "rk", msg, 1)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m, ok := store.Dequeue("q1")
		if !ok {
			b.Fatal("queue empty")
		}
		_ = deliverer.Deliver(m, "q1", 1)
		_ = tracker.Ack(uint64(i), 1)
	}
}

// BenchmarkConsumeAutoAck is identical to BenchmarkConsume but uses
// auto-ack consumers, removing the explicit ack tracking step.
func BenchmarkConsumeAutoAck(b *testing.B) {
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
	_, _ = cm.Subscribe("c1", "q1", ch, true, false, nil)

	deliverer := server.NewDeliverer(cm, store, tracker)

	msg := benchMessage(256)
	for i := 0; i < b.N+1000; i++ {
		msg.SetRoutingMeta("ex.direct", "rk")
		_, _ = pub.Publish("ex.direct", "rk", msg, 1)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m, ok := store.Dequeue("q1")
		if !ok {
			b.Fatal("queue empty")
		}
		_ = deliverer.Deliver(m, "q1", 1)
	}
}

// BenchmarkConsumeMultipleConsumers measures throughput with 4
// consumers sharing the same queue.
func BenchmarkConsumeMultipleConsumers(b *testing.B) {
	srv := benchServer()
	setupDirect(srv, "ex.direct", "q1", "rk")
	pub := srv.Publisher()
	store := srv.MessageStore()
	cm := srv.ConsumerManager()
	tracker := srv.DeliveryTracker()

	auth := server.NewMemoryAuthenticator()
	conn := server.NewConnection(nil, auth, nil)
	chM := server.NewChannelManager(10)

	const nConsumers = 4
	for i := 0; i < nConsumers; i++ {
		ch, _ := chM.Create(uint16(i+1), conn)
		ch.Open()
		_, _ = cm.Subscribe(
			fmt.Sprintf("c%d", i),
			"q1", ch, false, false, nil)
	}

	deliverer := server.NewDeliverer(cm, store, tracker)

	msg := benchMessage(256)
	for i := 0; i < b.N+1000; i++ {
		msg.SetRoutingMeta("ex.direct", "rk")
		_, _ = pub.Publish("ex.direct", "rk", msg, 1)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m, ok := store.Dequeue("q1")
		if !ok {
			b.Fatal("queue empty")
		}
		_ = deliverer.Deliver(m, "q1", 1)
		_ = tracker.Ack(uint64(i), 1)
	}
}
