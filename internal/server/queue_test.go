package server

import (
	"testing"
)

// TestQueueDeclare creates a new queue.
func TestQueueDeclare(t *testing.T) {
	mgr := NewQueueManager()
	q, err := mgr.Declare("test", true, false, false, nil, nil)
	if err != nil {
		t.Fatalf("declare: %v", err)
	}
	if q.Name != "test" {
		t.Fatalf("name = %q, want test", q.Name)
	}
	if q.State() != QueueActive {
		t.Fatalf("state = %d, want Active", q.State())
	}
}

// TestQueueDeclareIdempotent allows re-declare with same args.
func TestQueueDeclareIdempotent(t *testing.T) {
	mgr := NewQueueManager()
	_, err := mgr.Declare("test", true, false, false, nil, nil)
	if err != nil {
		t.Fatalf("first declare: %v", err)
	}
	_, err = mgr.Declare("test", true, false, false, nil, nil)
	if err != nil {
		t.Fatalf("second declare: %v", err)
	}
}

// TestQueueDeclareConflict rejects different args re-declare.
func TestQueueDeclareConflict(t *testing.T) {
	mgr := NewQueueManager()
	_, err := mgr.Declare("test", true, false, false, nil, nil)
	if err != nil {
		t.Fatalf("first declare: %v", err)
	}
	_, err = mgr.Declare("test", false, false, false, nil, nil)
	if err == nil {
		t.Fatal("expected error for arg conflict")
	}
}

// TestQueueDelete removes a queue.
func TestQueueDelete(t *testing.T) {
	mgr := NewQueueManager()
	_, err := mgr.Declare("test", true, false, false, nil, nil)
	if err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := mgr.Delete("test"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := mgr.Get("test"); ok {
		t.Fatal("queue should be deleted")
	}
}

// TestQueueExclusive marks queue as exclusive with owner.
func TestQueueExclusive(t *testing.T) {
	mgr := NewQueueManager()
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)
	q, err := mgr.Declare("excl", true, true, false, nil, conn)
	if err != nil {
		t.Fatalf("declare: %v", err)
	}
	if !q.IsExclusive() {
		t.Fatal("queue should be exclusive")
	}
	if q.Owner() != conn {
		t.Fatal("owner mismatch")
	}
}

// TestQueueRemoveExclusive deletes exclusive queues by owner.
func TestQueueRemoveExclusive(t *testing.T) {
	mgr := NewQueueManager()
	auth := NewMemoryAuthenticator()
	conn := NewConnection(nil, auth, nil)

	_, err := mgr.Declare("excl", true, true, false, nil, conn)
	if err != nil {
		t.Fatalf("declare exclusive: %v", err)
	}
	_, err = mgr.Declare("shared", true, false, false, nil, nil)
	if err != nil {
		t.Fatalf("declare shared: %v", err)
	}

	mgr.RemoveExclusive(conn)
	if mgr.Count() != 1 {
		t.Fatalf("count = %d, want 1 after RemoveExclusive", mgr.Count())
	}
}
