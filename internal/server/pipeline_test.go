// pipeline_test.go — unit tests for the batched pipeline.
package server

import (
	"testing"
	"time"
)

func TestPipeline_SubmitAndFlush(t *testing.T) {
	srv := NewServer()
	setupDirect(srv, "ex.direct", "q1", "rk")

	pipe := NewPipeline(srv.Publisher(), DefaultPipelineConfig())
	defer pipe.Stop()

	msg := NewMessage([]byte("pipe-test"), Properties{})
	msg.SetRoutingMeta("ex.direct", "rk")

	pipe.Submit("ex.direct", "rk", msg, 1)
	pipe.Flush()

	// After flush the message should be routed and enqueued.
	if srv.MessageStore().Len("q1") != 1 {
		t.Fatalf("expected 1 message in q1, got %d", srv.MessageStore().Len("q1"))
	}
}

func TestPipeline_BatchSizeRespected(t *testing.T) {
	srv := NewServer()
	setupDirect(srv, "ex.direct", "q1", "rk")

	cfg := PipelineConfig{BatchSize: 4, WorkerCount: 1, InputBuffer: 16}
	pipe := NewPipeline(srv.Publisher(), cfg)
	defer pipe.Stop()

	for i := 0; i < 10; i++ {
		msg := NewMessage([]byte("batch"), Properties{})
		msg.SetRoutingMeta("ex.direct", "rk")
		pipe.Submit("ex.direct", "rk", msg, 1)
	}
	pipe.Flush()

	if srv.MessageStore().Len("q1") != 10 {
		t.Fatalf("expected 10 messages, got %d", srv.MessageStore().Len("q1"))
	}
}

func TestPipeline_MultipleWorkers(t *testing.T) {
	srv := NewServer()
	setupDirect(srv, "ex.direct", "q1", "rk")

	cfg := PipelineConfig{BatchSize: 8, WorkerCount: 4, InputBuffer: 64}
	pipe := NewPipeline(srv.Publisher(), cfg)
	defer pipe.Stop()

	const n = 100
	for i := 0; i < n; i++ {
		msg := NewMessage([]byte("multi"), Properties{})
		msg.SetRoutingMeta("ex.direct", "rk")
		pipe.Submit("ex.direct", "rk", msg, 1)
	}
	pipe.Flush()

	if srv.MessageStore().Len("q1") != n {
		t.Fatalf("expected %d messages, got %d", n, srv.MessageStore().Len("q1"))
	}
}

func TestPipeline_StopDrainsRemaining(t *testing.T) {
	srv := NewServer()
	setupDirect(srv, "ex.direct", "q1", "rk")

	cfg := PipelineConfig{BatchSize: 64, WorkerCount: 1, InputBuffer: 16}
	pipe := NewPipeline(srv.Publisher(), cfg)

	for i := 0; i < 5; i++ {
		msg := NewMessage([]byte("drain"), Properties{})
		msg.SetRoutingMeta("ex.direct", "rk")
		pipe.Submit("ex.direct", "rk", msg, 1)
	}
	// Stop should drain remaining items.
	pipe.Stop()

	if srv.MessageStore().Len("q1") != 5 {
		t.Fatalf("expected 5 messages after stop, got %d", srv.MessageStore().Len("q1"))
	}
}

func TestPipeline_GracefulUnderBackpressure(t *testing.T) {
	srv := NewServer()
	setupDirect(srv, "ex.direct", "q1", "rk")

	cfg := PipelineConfig{BatchSize: 2, WorkerCount: 1, InputBuffer: 2}
	pipe := NewPipeline(srv.Publisher(), cfg)
	defer pipe.Stop()

	// Flood the small input buffer; submits should not panic or block
	// forever (they may drop if full, but should not deadlock).
	done := make(chan struct{})
	go func() {
		for i := 0; i < 20; i++ {
			msg := NewMessage([]byte("flood"), Properties{})
			msg.SetRoutingMeta("ex.direct", "rk")
			pipe.Submit("ex.direct", "rk", msg, 1)
		}
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock under backpressure")
	}
}

// setupDirect is a test helper matching the benchmark helper.
func setupDirect(s *Server, ex, q, rk string) {
	_, _ = s.ExchangeManager().Declare(ex, ExchangeDirect, false, false, false, nil)
	_, _ = s.QueueManager().Declare(q, false, false, false, nil, nil)
	_, _ = s.BindingManager().Bind(ex, q, rk, nil)
}

// BenchmarkPublishViaPipeline measures the throughput of the batched
// publish pipeline under ideal conditions (single exchange, single queue).
func BenchmarkPublishViaPipeline(b *testing.B) {
	srv := NewServer()
	setupDirect(srv, "ex.direct", "q1", "rk")

	msg := NewMessage([]byte("benchmark"), Properties{})
	msg.SetRoutingMeta("ex.direct", "rk")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srv.PublishViaPipeline("ex.direct", "rk", msg, 1)
	}
	srv.Pipeline().Flush()
	b.StopTimer()

	if srv.MessageStore().Len("q1") != b.N {
		b.Fatalf("expected %d messages, got %d", b.N, srv.MessageStore().Len("q1"))
	}
}
