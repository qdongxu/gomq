// quorum_test.go — Quorum Queue multi-message publish/consume
// consistency integration.
package integration

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/qdongxu/gomq/internal/cluster"
	"github.com/qdongxu/gomq/internal/queue"
)

// TestQuorumQueueMultiPublishConsume verifies multiple messages
// published through Raft are returned in order by Consume.
func TestQuorumQueueMultiPublishConsume(t *testing.T) {
	r := cluster.NewRaftNode("n1", nil)
	r.BecomeLeader()

	qq := queue.NewQuorumQueue("q1", r)

	// Publish 5 messages.
	want := []string{"a", "b", "c", "d", "e"}
	for i, body := range want {
		go func(idx int) {
			r.SetCommitIndex(uint64(idx + 1))
			qq.OnCommit(uint64(idx + 1))
		}(i)

		if err := qq.Publish([]byte(body)); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	msgs := qq.Consume(10)
	if len(msgs) != 5 {
		t.Fatalf("consume len = %d, want 5", len(msgs))
	}
	for i, expected := range want {
		if !bytes.Equal(msgs[i], []byte(expected)) {
			t.Fatalf("msg %d = %q, want %q", i, msgs[i], expected)
		}
	}
}

// TestQuorumQueueAckRemovesMessage confirms that acking a consumed
// message prevents it from reappearing.
func TestQuorumQueueAckRemovesMessage(t *testing.T) {
	r := cluster.NewRaftNode("n1", nil)
	r.BecomeLeader()

	qq := queue.NewQuorumQueue("q1", r)

	go func() {
		r.SetCommitIndex(1)
		qq.OnCommit(1)
	}()
	if err := qq.Publish([]byte("ack-me")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs := qq.Consume(10)
	if len(msgs) != 1 {
		t.Fatalf("consume len = %d, want 1", len(msgs))
	}

	qq.Ack(1)

	msgs2 := qq.Consume(10)
	if len(msgs2) != 0 {
		t.Fatalf("expected empty after ack, got %d", len(msgs2))
	}
}

// TestQuorumQueueNotLeaderBlocksPublish ensures that a non-leader
// node refuses to publish.
func TestQuorumQueueNotLeaderBlocksPublish(t *testing.T) {
	r := cluster.NewRaftNode("n1", []string{"n2", "n3"})
	// Not leader.
	qq := queue.NewQuorumQueue("q1", r)
	if err := qq.Publish([]byte("fail")); err == nil {
		t.Fatal("expected error from non-leader")
	}
}

// TestQuorumQueueManagerLifecycle tests create / get / delete
// through the manager.
func TestQuorumQueueManagerLifecycle(t *testing.T) {
	mgr := queue.NewQuorumQueueManager(nil)

	qq, err := mgr.Create("qq1", "n1")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !qq.IsLeader() {
		t.Fatal("expected leader")
	}

	q2, ok := mgr.Get("qq1")
	if !ok || q2 != qq {
		t.Fatal("get failed")
	}

	if err := mgr.Delete("qq1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := mgr.Get("qq1"); ok {
		t.Fatal("expected not found after delete")
	}
}

// TestQuorumQueueDuplicateCreation fails when the same queue name
// is created twice.
func TestQuorumQueueDuplicateCreation(t *testing.T) {
	mgr := queue.NewQuorumQueueManager(nil)
	_, err := mgr.Create("dup", "n1")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = mgr.Create("dup", "n1")
	if err == nil {
		t.Fatal("expected duplicate error")
	}
}

// TestQuorumQueuePayloadIntegrity verifies the exact payload bytes
// survive the Raft propose / commit cycle.
func TestQuorumQueuePayloadIntegrity(t *testing.T) {
	r := cluster.NewRaftNode("n1", nil)
	r.BecomeLeader()

	qq := queue.NewQuorumQueue("q1", r)

	payload := []byte("binary\x00\x01\xffend")
	go func() {
		r.SetCommitIndex(1)
		qq.OnCommit(1)
	}()
	if err := qq.Publish(payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs := qq.Consume(10)
	if len(msgs) != 1 {
		t.Fatalf("consume len = %d, want 1", len(msgs))
	}
	if !bytes.Equal(msgs[0], payload) {
		t.Fatalf("payload corrupted")
	}
}

// TestQuorumQueuePartialConsume requests fewer messages than
// available and verifies the remainder stays in the queue.
func TestQuorumQueuePartialConsume(t *testing.T) {
	r := cluster.NewRaftNode("n1", nil)
	r.BecomeLeader()

	qq := queue.NewQuorumQueue("q1", r)

	for i := 1; i <= 5; i++ {
		go func(idx int) {
			r.SetCommitIndex(uint64(idx))
			qq.OnCommit(uint64(idx))
		}(i)
		if err := qq.Publish([]byte(fmt.Sprintf("msg%d", i))); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	msgs := qq.Consume(2)
	if len(msgs) != 2 {
		t.Fatalf("first consume = %d, want 2", len(msgs))
	}

	qq.Ack(2)

	msgs2 := qq.Consume(10)
	if len(msgs2) != 3 {
		t.Fatalf("second consume = %d, want 3", len(msgs2))
	}
}
