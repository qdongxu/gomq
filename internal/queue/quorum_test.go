// quorum_test.go tests the quorum queue implementation.
package queue

import (
	"testing"

	"github.com/qdongxu/gomq/internal/cluster"
)

func TestQuorumQueueBasicPublishConsume(t *testing.T) {
	r := cluster.NewRaftNode("n1", nil)
	r.BecomeLeader()

	qq := NewQuorumQueue("q1", r)

	// Simulate async commit.
	go func() {
		r.SetCommitIndex(1)
		qq.OnCommit(1)
	}()

	if err := qq.Publish([]byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs := qq.Consume(10)
	if len(msgs) != 1 || string(msgs[0]) != "hello" {
		t.Fatalf("consume = %v, want [hello]", msgs)
	}
}

func TestQuorumQueueNotLeader(t *testing.T) {
	r := cluster.NewRaftNode("n1", []string{"n2", "n3"})
	// Not leader.
	qq := NewQuorumQueue("q1", r)
	if err := qq.Publish([]byte("hello")); err == nil {
		t.Fatal("expected error from non-leader")
	}
}

func TestQuorumQueueManager(t *testing.T) {
	mgr := NewQuorumQueueManager(nil)
	qq, err := mgr.Create("q1", "n1")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !qq.IsLeader() {
		t.Fatal("expected leader")
	}

	q2, ok := mgr.Get("q1")
	if !ok || q2 != qq {
		t.Fatal("get failed")
	}

	if err := mgr.Delete("q1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := mgr.Get("q1"); ok {
		t.Fatal("expected not found after delete")
	}
}

func TestQuorumQueueManagerDuplicate(t *testing.T) {
	mgr := NewQuorumQueueManager(nil)
	_, err := mgr.Create("q1", "n1")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, err = mgr.Create("q1", "n1")
	if err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestQuorumQueueAck(t *testing.T) {
	r := cluster.NewRaftNode("n1", nil)
	r.BecomeLeader()

	qq := NewQuorumQueue("q1", r)

	// Publish and commit async.
	for i := 0; i < 3; i++ {
		go func(idx uint64) {
			r.SetCommitIndex(idx)
			qq.OnCommit(idx)
		}(uint64(i + 1))
		if err := qq.Publish([]byte("msg")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	qq.Ack(2)
	msgs := qq.Consume(10)
	if len(msgs) != 1 {
		t.Fatalf("after ack, len = %d, want 1", len(msgs))
	}
}

func TestRaftNodeStateTransitions(t *testing.T) {
	r := cluster.NewRaftNode("n1", nil)
	if r.State() != cluster.StateFollower {
		t.Fatalf("initial state = %v", r.State())
	}

	r.BecomeCandidate()
	if r.State() != cluster.StateCandidate {
		t.Fatalf("after candidate = %v", r.State())
	}
	if r.Term() != 1 {
		t.Fatalf("term = %d, want 1", r.Term())
	}

	r.BecomeLeader()
	if !r.IsLeader() {
		t.Fatal("expected leader")
	}
}

func TestRaftNodeAppendEntries(t *testing.T) {
	r := cluster.NewRaftNode("n1", nil)
	if !r.AppendEntries(1, 0, []cluster.LogEntry{
		{Index: 1, Term: 1, Command: []byte("a")},
	}) {
		t.Fatal("append rejected")
	}
	if r.CommitIndex() != 0 {
		t.Fatalf("commit = %d, want 0", r.CommitIndex())
	}
	// Commit via leaderCommit.
	r.AppendEntries(1, 1, nil)
	if r.CommitIndex() != 1 {
		t.Fatalf("commit = %d, want 1", r.CommitIndex())
	}
}

func TestRaftNodeRequestVote(t *testing.T) {
	r := cluster.NewRaftNode("n1", nil)
	// n1 is follower, term 0.
	if !r.RequestVote(1, "n2", 0, 0) {
		t.Fatal("vote should be granted")
	}
	if r.RequestVote(1, "n3", 0, 0) {
		t.Fatal("already voted, should reject")
	}
	// Lower term.
	if r.RequestVote(0, "n4", 0, 0) {
		t.Fatal("lower term should reject")
	}
}

func TestRaftNodeCommitted(t *testing.T) {
	r := cluster.NewRaftNode("n1", nil)
	r.BecomeLeader()
	idx, _ := r.Propose([]byte("x"))
	if idx != 1 {
		t.Fatalf("idx = %d, want 1", idx)
	}
	r.SetCommitIndex(1)
	committed := r.Committed()
	if len(committed) != 1 || string(committed[0].Command) != "x" {
		t.Fatalf("committed = %v", committed)
	}
}
