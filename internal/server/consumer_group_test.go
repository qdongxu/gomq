// consumer_group_test.go tests consumer groups and strategies.
package server

import (
	"testing"
)

func TestNewConsumerGroup(t *testing.T) {
	g := NewConsumerGroup("g1", "q1", "round-robin")
	if g.ID() != "g1" {
		t.Fatalf("id = %q, want g1", g.ID())
	}
	if g.Queue() != "q1" {
		t.Fatalf("queue = %q, want q1", g.Queue())
	}
	if g.StrategyName() != "round-robin" {
		t.Fatalf("strategy = %q, want round-robin", g.StrategyName())
	}
	if g.Count() != 0 {
		t.Fatalf("count = %d, want 0", g.Count())
	}
}

func TestConsumerGroupAddRemove(t *testing.T) {
	g := NewConsumerGroup("g1", "q1", "round-robin")
	c1 := NewConsumer("c1", "q1", nil, false, false, false, nil, "g1")
	c2 := NewConsumer("c2", "q1", nil, false, false, false, nil, "g1")

	g.Add(c1)
	if g.Count() != 1 {
		t.Fatalf("count = %d, want 1", g.Count())
	}

	g.Add(c2)
	if g.Count() != 2 {
		t.Fatalf("count = %d, want 2", g.Count())
	}

	if !g.Remove("c1") {
		t.Fatal("expected c1 to be removed")
	}
	if g.Count() != 1 {
		t.Fatalf("count = %d, want 1", g.Count())
	}

	if g.Remove("c1") {
		t.Fatal("expected c1 removal to fail (already gone)")
	}
}

func TestRoundRobinStrategy(t *testing.T) {
	rr := &RoundRobinStrategy{}
	c1 := NewConsumer("c1", "q1", nil, false, false, false, nil, "g1")
	c2 := NewConsumer("c2", "q1", nil, false, false, false, nil, "g1")
	c3 := NewConsumer("c3", "q1", nil, false, false, false, nil, "g1")
	members := []*Consumer{c1, c2, c3}

	for i := 0; i < 6; i++ {
		c := rr.Select(members, "rk")
		if c == nil {
			t.Fatalf("nil consumer at i=%d", i)
		}
		expected := members[i%3]
		if c.Tag() != expected.Tag() {
			t.Fatalf("i=%d: got %q, want %q", i, c.Tag(), expected.Tag())
		}
	}
}

func TestRoundRobinStrategyEmpty(t *testing.T) {
	rr := &RoundRobinStrategy{}
	if c := rr.Select(nil, "rk"); c != nil {
		t.Fatal("expected nil for empty members")
	}
}

func TestHashStrategy(t *testing.T) {
	hs := &HashStrategy{}
	c1 := NewConsumer("c1", "q1", nil, false, false, false, nil, "g1")
	c2 := NewConsumer("c2", "q1", nil, false, false, false, nil, "g1")
	members := []*Consumer{c1, c2}

	// Same key should always select the same consumer.
	first := hs.Select(members, "foo")
	for i := 0; i < 10; i++ {
		c := hs.Select(members, "foo")
		if c.Tag() != first.Tag() {
			t.Fatalf("hash inconsistent: got %q, want %q", c.Tag(), first.Tag())
		}
	}

	// Different key may select different consumer.
	other := hs.Select(members, "bar")
	if other == nil {
		t.Fatal("expected non-nil consumer")
	}
}

func TestHashStrategyEmpty(t *testing.T) {
	hs := &HashStrategy{}
	if c := hs.Select(nil, "rk"); c != nil {
		t.Fatal("expected nil for empty members")
	}
}

func TestConsumerGroupSelect(t *testing.T) {
	g := NewConsumerGroup("g1", "q1", "round-robin")
	c1 := NewConsumer("c1", "q1", nil, false, false, false, nil, "g1")
	c2 := NewConsumer("c2", "q1", nil, false, false, false, nil, "g1")
	g.Add(c1)
	g.Add(c2)

	seen := make(map[string]bool)
	for i := 0; i < 4; i++ {
		c := g.Select("rk")
		if c == nil {
			t.Fatalf("nil consumer at i=%d", i)
		}
		seen[c.Tag()] = true
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 unique consumers, got %d", len(seen))
	}
}

func TestConsumerGroupHashSelect(t *testing.T) {
	g := NewConsumerGroup("g1", "q1", "hash")
	c1 := NewConsumer("c1", "q1", nil, false, false, false, nil, "g1")
	c2 := NewConsumer("c2", "q1", nil, false, false, false, nil, "g1")
	g.Add(c1)
	g.Add(c2)

	first := g.Select("foo")
	for i := 0; i < 5; i++ {
		c := g.Select("foo")
		if c.Tag() != first.Tag() {
			t.Fatalf("hash inconsistent: got %q, want %q", c.Tag(), first.Tag())
		}
	}
}

func TestConsumerGroupManagerCreate(t *testing.T) {
	m := NewConsumerGroupManager()
	g := m.Create("g1", "q1", "round-robin")
	if g == nil {
		t.Fatal("expected non-nil group")
	}
	if g.ID() != "g1" {
		t.Fatalf("id = %q", g.ID())
	}

	// Duplicate creation returns nil.
	if m.Create("g1", "q1", "hash") != nil {
		t.Fatal("expected nil for duplicate group")
	}
}

func TestConsumerGroupManagerGet(t *testing.T) {
	m := NewConsumerGroupManager()
	m.Create("g1", "q1", "round-robin")

	g, ok := m.Get("g1")
	if !ok || g.ID() != "g1" {
		t.Fatal("expected to find g1")
	}

	_, ok = m.Get("g2")
	if ok {
		t.Fatal("expected g2 not found")
	}
}

func TestConsumerGroupManagerGetByQueue(t *testing.T) {
	m := NewConsumerGroupManager()
	m.Create("g1", "q1", "round-robin")
	m.Create("g2", "q1", "hash")
	m.Create("g3", "q2", "round-robin")

	groups := m.GetByQueue("q1")
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups for q1, got %d", len(groups))
	}

	groups = m.GetByQueue("q2")
	if len(groups) != 1 {
		t.Fatalf("expected 1 group for q2, got %d", len(groups))
	}

	groups = m.GetByQueue("q3")
	if len(groups) != 0 {
		t.Fatalf("expected 0 groups for q3, got %d", len(groups))
	}
}

func TestConsumerGroupManagerJoin(t *testing.T) {
	m := NewConsumerGroupManager()
	c := NewConsumer("c1", "q1", nil, false, false, false, nil, "g1")
	g := m.Join("g1", "q1", c, "round-robin")
	if g == nil {
		t.Fatal("expected non-nil group")
	}
	if g.Count() != 1 {
		t.Fatalf("count = %d, want 1", g.Count())
	}

	// Join existing group.
	c2 := NewConsumer("c2", "q1", nil, false, false, false, nil, "g1")
	g2 := m.Join("g1", "q1", c2, "round-robin")
	if g2.ID() != "g1" {
		t.Fatalf("id = %q", g2.ID())
	}
	if g2.Count() != 2 {
		t.Fatalf("count = %d, want 2", g2.Count())
	}
}

func TestConsumerGroupManagerLeave(t *testing.T) {
	m := NewConsumerGroupManager()
	c1 := NewConsumer("c1", "q1", nil, false, false, false, nil, "g1")
	c2 := NewConsumer("c2", "q1", nil, false, false, false, nil, "g1")
	m.Join("g1", "q1", c1, "round-robin")
	m.Join("g1", "q1", c2, "round-robin")

	if !m.Leave("c1") {
		t.Fatal("expected c1 to leave")
	}
	g, _ := m.Get("g1")
	if g.Count() != 1 {
		t.Fatalf("count = %d, want 1", g.Count())
	}

	// Leave last member → group deleted.
	if !m.Leave("c2") {
		t.Fatal("expected c2 to leave")
	}
	_, ok := m.Get("g1")
	if ok {
		t.Fatal("expected g1 to be deleted after last member leaves")
	}
}

func TestConsumerGroupManagerDelete(t *testing.T) {
	m := NewConsumerGroupManager()
	m.Create("g1", "q1", "round-robin")
	if !m.Delete("g1") {
		t.Fatal("expected delete to succeed")
	}
	if m.Delete("g1") {
		t.Fatal("expected delete to fail (already gone)")
	}
}

func TestConsumerGroupManagerSelect(t *testing.T) {
	m := NewConsumerGroupManager()
	c1 := NewConsumer("c1", "q1", nil, false, false, false, nil, "g1")
	c2 := NewConsumer("c2", "q1", nil, false, false, false, nil, "g1")
	m.Join("g1", "q1", c1, "round-robin")
	m.Join("g1", "q1", c2, "round-robin")

	seen := make(map[string]bool)
	for i := 0; i < 4; i++ {
		c := m.Select("g1", "rk")
		if c == nil {
			t.Fatalf("nil consumer at i=%d", i)
		}
		seen[c.Tag()] = true
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 unique consumers, got %d", len(seen))
	}
}

func TestConsumerGroupManagerSelectUnknown(t *testing.T) {
	m := NewConsumerGroupManager()
	if c := m.Select("g1", "rk"); c != nil {
		t.Fatal("expected nil for unknown group")
	}
}

func TestConsumerGroupManagerList(t *testing.T) {
	m := NewConsumerGroupManager()
	m.Create("g1", "q1", "round-robin")
	m.Create("g2", "q2", "hash")

	list := m.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(list))
	}
}

func TestConsumerWithGroupID(t *testing.T) {
	c := NewConsumer("c1", "q1", nil, true, false, false, nil, "my-group")
	if c.GroupID() != "my-group" {
		t.Fatalf("groupID = %q, want my-group", c.GroupID())
	}
	if c.Tag() != "c1" {
		t.Fatalf("tag = %q, want c1", c.Tag())
	}
	if c.QueueName() != "q1" {
		t.Fatalf("queue = %q, want q1", c.QueueName())
	}
	if !c.AutoAck() {
		t.Fatal("expected autoAck=true")
	}
}

func TestConsumerWithoutGroupID(t *testing.T) {
	c := NewConsumer("c1", "q1", nil, false, false, false, nil, "")
	if c.GroupID() != "" {
		t.Fatalf("groupID = %q, want empty", c.GroupID())
	}
}
