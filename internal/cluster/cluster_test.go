// cluster_test.go tests the clustering foundation.
package cluster

import (
	"testing"
)

func TestNewCluster(t *testing.T) {
	c := NewCluster("n1", "127.0.0.1:5672")
	if c.LocalID() != "n1" {
		t.Fatalf("localID = %q, want n1", c.LocalID())
	}
	if c.LocalAddr() != "127.0.0.1:5672" {
		t.Fatalf("localAddr = %q", c.LocalAddr())
	}
	if !c.IsLeader() {
		t.Fatal("new cluster should be leader")
	}
	if c.Leader() != "n1" {
		t.Fatalf("leader = %q, want n1", c.Leader())
	}
	if c.NodeCount() != 1 {
		t.Fatalf("count = %d, want 1", c.NodeCount())
	}
}

func TestJoinLeave(t *testing.T) {
	c := NewCluster("n1", "127.0.0.1:5672")
	c.Join("n2", "127.0.0.1:5673")

	if c.NodeCount() != 2 {
		t.Fatalf("count = %d, want 2", c.NodeCount())
	}

	nodes := c.Nodes()
	if len(nodes) != 2 {
		t.Fatalf("nodes len = %d, want 2", len(nodes))
	}

	c.Leave("n2")
	if c.NodeCount() != 1 {
		t.Fatalf("count after leave = %d, want 1", c.NodeCount())
	}
}

func TestLeaderFailover(t *testing.T) {
	c := NewCluster("n1", "127.0.0.1:5672")
	c.Join("n2", "127.0.0.1:5673")
	c.leader = "n2"

	if c.Leader() != "n2" {
		t.Fatalf("leader = %q, want n2", c.Leader())
	}

	c.Leave("n2")
	if c.Leader() != "n1" {
		t.Fatalf("leader after leave = %q, want n1", c.Leader())
	}
}

func TestHeartbeat(t *testing.T) {
	c := NewCluster("n1", "127.0.0.1:5672")
	c.Join("n2", "127.0.0.1:5673")

	before := c.Nodes()[1].LastSeen
	c.Heartbeat("n2")
	after := c.Nodes()[1].LastSeen

	if !after.After(before) {
		t.Fatal("heartbeat did not update LastSeen")
	}
}
