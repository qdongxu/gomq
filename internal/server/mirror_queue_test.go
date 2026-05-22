// mirror_queue_test.go tests the MirroredQueue type.
package server

import (
	"testing"
)

func TestNewMirroredQueue(t *testing.T) {
	q := NewQueue("q1", true, false, false, nil, nil)
	mq := NewMirroredQueue(q, []string{"node-a", "node-b"})
	if mq.Queue != q {
		t.Fatal("MirroredQueue.Queue mismatch")
	}
	if len(mq.MirrorTo()) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(mq.MirrorTo()))
	}
}

func TestMirroredQueue_AddMirror(t *testing.T) {
	q := NewQueue("q1", true, false, false, nil, nil)
	mq := NewMirroredQueue(q, []string{"node-a"})

	mq.AddMirror("node-b")
	peers := mq.MirrorTo()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}

	mq.AddMirror("node-b") // duplicate
	peers = mq.MirrorTo()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers after dup, got %d", len(peers))
	}
}

func TestMirroredQueue_RemoveMirror(t *testing.T) {
	q := NewQueue("q1", true, false, false, nil, nil)
	mq := NewMirroredQueue(q, []string{"node-a", "node-b"})

	mq.RemoveMirror("node-a")
	peers := mq.MirrorTo()
	if len(peers) != 1 || peers[0] != "node-b" {
		t.Fatalf("unexpected peers: %v", peers)
	}

	mq.RemoveMirror("node-c") // non-existent
	peers = mq.MirrorTo()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
}

func TestMirroredQueue_SetMirrorTo(t *testing.T) {
	q := NewQueue("q1", true, false, false, nil, nil)
	mq := NewMirroredQueue(q, []string{"node-a"})

	mq.SetMirrorTo([]string{"node-x", "node-y"})
	peers := mq.MirrorTo()
	if len(peers) != 2 || peers[0] != "node-x" {
		t.Fatalf("unexpected peers: %v", peers)
	}
}

func TestMirroredQueue_Broadcast(t *testing.T) {
	q := NewQueue("q1", true, false, false, nil, nil)
	mq := NewMirroredQueue(q, []string{"node-a"})

	if err := mq.Broadcast(nil); err != nil {
		t.Fatalf("broadcast: %v", err)
	}

	mq2 := NewMirroredQueue(q, []string{})
	if err := mq2.Broadcast(nil); err != nil {
		t.Fatalf("broadcast empty: %v", err)
	}
}
