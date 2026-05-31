package cluster

import (
	"fmt"
	"testing"
	"time"
)

func TestRaftThreeNodeLeaderElection(t *testing.T) {
	trans := NewMemoryTransport()

	nodes := make([]*RaftNode, 3)
	stopChs := make([]chan struct{}, 3)

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node-%d", i)
		peers := []string{}
		for j := 0; j < 3; j++ {
			if j != i {
				peers = append(peers, fmt.Sprintf("node-%d", j))
			}
		}
		nodes[i] = NewRaftNode(id, peers)
		nodes[i].SetTransport(trans)
		trans.RegisterPeer(id, id, nodes[i])
		stopChs[i] = make(chan struct{})
		go nodes[i].Run(stopChs[i])
	}
	defer func() {
		for i := range stopChs {
			close(stopChs[i])
			nodes[i].Stop()
		}
	}()

	// Wait for leader election.
	time.Sleep(500 * time.Millisecond)

	leader := findLeader(nodes)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	if !leader.IsLeader() {
		t.Fatal("leader should be leader")
	}
}

func TestRaftThreeNodeLogReplication(t *testing.T) {
	trans := NewMemoryTransport()

	nodes := make([]*RaftNode, 3)
	stopChs := make([]chan struct{}, 3)

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node-%d", i)
		peers := []string{}
		for j := 0; j < 3; j++ {
			if j != i {
				peers = append(peers, fmt.Sprintf("node-%d", j))
			}
		}
		nodes[i] = NewRaftNode(id, peers)
		nodes[i].SetTransport(trans)
		trans.RegisterPeer(id, id, nodes[i])
		stopChs[i] = make(chan struct{})
		go nodes[i].Run(stopChs[i])
	}
	defer func() {
		for i := range stopChs {
			close(stopChs[i])
			nodes[i].Stop()
		}
	}()

	// Wait for leader election.
	time.Sleep(500 * time.Millisecond)

	leader := findLeader(nodes)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	// Propose an entry.
	idx, err := leader.Propose([]byte("test-cmd"))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if idx != 1 {
		t.Fatalf("idx = %d, want 1", idx)
	}

	// Manually advance commit index (simplified Raft: leader commits immediately).
	leader.SetCommitIndex(idx)

	// Replicate to followers.
	leader.ReplicateAll()

	// Wait for replication.
	time.Sleep(200 * time.Millisecond)

	// Verify all nodes have the entry.
	for _, n := range nodes {
		committed := n.Committed()
		if len(committed) == 0 || string(committed[len(committed)-1].Command) != "test-cmd" {
			t.Fatalf("node %s missing committed command", n.nodeID)
		}
	}
}

func TestRaftThreeNodeFailover(t *testing.T) {
	trans := NewMemoryTransport()

	nodes := make([]*RaftNode, 3)
	stopChs := make([]chan struct{}, 3)

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node-%d", i)
		peers := []string{}
		for j := 0; j < 3; j++ {
			if j != i {
				peers = append(peers, fmt.Sprintf("node-%d", j))
			}
		}
		nodes[i] = NewRaftNode(id, peers)
		nodes[i].SetTransport(trans)
		trans.RegisterPeer(id, id, nodes[i])
		stopChs[i] = make(chan struct{})
		go nodes[i].Run(stopChs[i])
	}
	defer func() {
		for i := range stopChs {
			select {
			case <-stopChs[i]:
			default:
				close(stopChs[i])
			}
			nodes[i].Stop()
		}
	}()

	// Wait for leader election.
	time.Sleep(500 * time.Millisecond)

	leader := findLeader(nodes)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	leaderID := leader.nodeID

	// Stop the leader and remove from transport.
	for i, n := range nodes {
		if n == leader {
			close(stopChs[i])
			nodes[i].Stop()
			trans.RemovePeer(n.nodeID)
			break
		}
	}

	// Wait for new leader election.
	time.Sleep(800 * time.Millisecond)

	// Find new leader among remaining nodes (exclude old leader).
	var newLeader *RaftNode
	for _, n := range nodes {
		if n.nodeID != leaderID && n.IsLeader() {
			newLeader = n
			break
		}
	}
	if newLeader == nil {
		t.Fatal("no new leader elected after failover")
	}
}

func findLeader(nodes []*RaftNode) *RaftNode {
	for _, n := range nodes {
		if n.IsLeader() {
			return n
		}
	}
	return nil
}
