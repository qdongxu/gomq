// message_trace_test.go tests the message tracer ring buffer.
package server

import (
	"testing"
)

func TestMessageTracerDisabled(t *testing.T) {
	mt := NewMessageTracer(false, 10)
	mt.TracePublished("msg-1", "ex1", "rk", "node-1")
	if mt.Count() != 0 {
		t.Fatalf("expected 0 traces when disabled, got %d", mt.Count())
	}
}

func TestMessageTracerLifecycle(t *testing.T) {
	mt := NewMessageTracer(true, 10)
	mt.TracePublished("msg-1", "ex1", "rk", "node-1")
	mt.TraceRouted("msg-1", "ex1", "rk", "q1", "node-1")
	mt.TraceDelivered("msg-1", "q1", "node-1")
	mt.TraceAcked("msg-1", "q1", "node-1")

	if mt.Count() != 4 {
		t.Fatalf("expected 4 traces, got %d", mt.Count())
	}

	all := mt.All()
	stages := []string{}
	for _, tr := range all {
		stages = append(stages, tr.Stage)
	}
	want := []string{"published", "routed", "delivered", "acked"}
	for i, w := range want {
		if stages[i] != w {
			t.Fatalf("stage %d = %s, want %s", i, stages[i], w)
		}
	}
	if all[0].Timestamp.IsZero() {
		t.Fatal("expected timestamp set")
	}
}

func TestMessageTracerRingBuffer(t *testing.T) {
	mt := NewMessageTracer(true, 2)
	mt.TracePublished("m1", "ex", "rk", "n1")
	mt.TracePublished("m2", "ex", "rk", "n1")
	mt.TracePublished("m3", "ex", "rk", "n1")

	if mt.Count() != 2 {
		t.Fatalf("expected 2 after overflow, got %d", mt.Count())
	}

	all := mt.All()
	if all[0].MessageID != "m2" || all[1].MessageID != "m3" {
		t.Fatalf("unexpected ring buffer contents: %+v", all)
	}
}

func TestMessageTracerNackReject(t *testing.T) {
	mt := NewMessageTracer(true, 10)
	mt.TraceNacked("msg-1", "q1", "node-1", false)
	mt.TraceRejected("msg-2", "q1", "node-1", true)
	mt.TraceDeadLetter("msg-3", "q1", "dlx", "node-1")

	all := mt.All()
	if len(all) != 3 {
		t.Fatalf("expected 3 traces, got %d", len(all))
	}
	if all[0].Stage != string(TraceNacked) {
		t.Fatalf("expected nacked, got %s", all[0].Stage)
	}
	if all[1].Stage != string(TraceRejected) {
		t.Fatalf("expected rejected, got %s", all[1].Stage)
	}
	if all[2].Stage != string(TraceDeadLetter) {
		t.Fatalf("expected dead_letter, got %s", all[2].Stage)
	}
}

func TestMessageTracerSetEnabled(t *testing.T) {
	mt := NewMessageTracer(false, 10)
	mt.SetEnabled(true)
	mt.TracePublished("m1", "ex", "rk", "n1")
	if mt.Count() != 1 {
		t.Fatalf("expected 1 after enabling, got %d", mt.Count())
	}
	mt.SetEnabled(false)
	mt.TracePublished("m2", "ex", "rk", "n1")
	if mt.Count() != 1 {
		t.Fatalf("expected 1 after disabling, got %d", mt.Count())
	}
}

func TestMessageTracerUnlimitedSize(t *testing.T) {
	mt := NewMessageTracer(true, 0)
	for i := 0; i < 5; i++ {
		mt.TracePublished("m", "ex", "rk", "n")
	}
	if mt.Count() != 5 {
		t.Fatalf("expected 5 with unlimited buffer, got %d", mt.Count())
	}
}
