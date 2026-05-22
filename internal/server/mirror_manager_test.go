// mirror_manager_test.go tests the MirrorManager type.
package server

import (
	"testing"
	"time"
)

func TestMirrorManager_Register(t *testing.T) {
	mm := NewMirrorManager()
	q := NewQueue("q1", true, false, false, nil, nil)
	mm.Register("q1", q, []string{"node-a"})

	if mm.Count() != 1 {
		t.Fatalf("expected count 1, got %d", mm.Count())
	}

	mq, ok := mm.Get("q1")
	if !ok {
		t.Fatal("expected q1 to exist")
	}
	if len(mq.MirrorTo()) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(mq.MirrorTo()))
	}
}

func TestMirrorManager_Unregister(t *testing.T) {
	mm := NewMirrorManager()
	q := NewQueue("q1", true, false, false, nil, nil)
	mm.Register("q1", q, []string{"node-a"})
	mm.Unregister("q1")

	if mm.Count() != 0 {
		t.Fatalf("expected count 0, got %d", mm.Count())
	}

	_, ok := mm.Get("q1")
	if ok {
		t.Fatal("expected q1 to be removed")
	}
}

func TestMirrorManager_Names(t *testing.T) {
	mm := NewMirrorManager()
	q1 := NewQueue("q1", true, false, false, nil, nil)
	q2 := NewQueue("q2", true, false, false, nil, nil)
	mm.Register("q1", q1, []string{"node-a"})
	mm.Register("q2", q2, []string{"node-b"})

	names := mm.Names()
	if len(names) != 2 {
		t.Fatalf("expected 2 names, got %d", len(names))
	}
}

func TestMirrorManager_SyncAll(t *testing.T) {
	mm := NewMirrorManager()
	q := NewQueue("q1", true, false, false, nil, nil)
	mm.Register("q1", q, []string{"node-a", "node-b"})

	mm.SyncAll() // should not panic
}

func TestMirrorManager_StartSyncLoop(t *testing.T) {
	mm := NewMirrorManager()
	q := NewQueue("q1", true, false, false, nil, nil)
	mm.Register("q1", q, []string{"node-a"})

	stop := mm.StartSyncLoop(50 * time.Millisecond)
	time.Sleep(120 * time.Millisecond)
	stop()

	// No panic and at least one sync should have run.
}
