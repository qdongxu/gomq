// cluster_test.go tests the clustering foundation.
package cluster

import (
	"encoding/json"
	"testing"
)

func TestNewCluster(t *testing.T) {
	c := NewCluster("n1", "127.0.0.1:5672")
	if c.LocalID() != "n1" {
		t.Fatalf("localID = %q, want n1", c.LocalID())
	}
	if c.LocalAddr() != "127.0.0.1:5672" {
		t.Fatalf("localAddr = %q, want 127.0.0.1:5672", c.LocalAddr())
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

func TestMembershipJoinLeave(t *testing.T) {
	m := NewMembership()
	m.Join("n1", "127.0.0.1:5672")
	m.Join("n2", "127.0.0.1:5673")

	if m.OnlineCount() != 2 {
		t.Fatalf("online = %d, want 2", m.OnlineCount())
	}

	mem, ok := m.Get("n1")
	if !ok || mem.Status != StatusOnline {
		t.Fatalf("n1 status = %v, want online", mem.Status)
	}

	m.Leave("n2")
	mem, _ = m.Get("n2")
	if mem.Status != StatusOffline {
		t.Fatalf("n2 status = %v, want offline", mem.Status)
	}
}

func TestMembershipSuspect(t *testing.T) {
	m := NewMembership()
	m.Join("n1", "127.0.0.1:5672")
	m.Suspect("n1")

	mem, _ := m.Get("n1")
	if mem.Status != StatusSuspected {
		t.Fatalf("status = %v, want suspected", mem.Status)
	}
}

func TestStatusString(t *testing.T) {
	if StatusOnline.String() != "online" {
		t.Fatalf("online string = %q", StatusOnline.String())
	}
	if StatusSuspected.String() != "suspected" {
		t.Fatalf("suspected string = %q", StatusSuspected.String())
	}
	if StatusOffline.String() != "offline" {
		t.Fatalf("offline string = %q", StatusOffline.String())
	}
}

func TestEventTypeConversion(t *testing.T) {
	if eventType(0) != EventPut {
		t.Fatal("put mismatch")
	}
	if eventType(1) != EventDelete {
		t.Fatal("delete mismatch")
	}
}

func TestNodeInfoMarshal(t *testing.T) {
	info := NodeInfo{ID: "n1", Addr: "127.0.0.1:5672"}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out NodeInfo
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.ID != "n1" || out.Addr != "127.0.0.1:5672" {
		t.Fatalf("unexpected: %+v", out)
	}
}
