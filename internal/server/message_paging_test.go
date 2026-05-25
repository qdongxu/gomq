// message_paging_test.go tests on-disk page file management.
package server

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPageManagerFlushAndLoad(t *testing.T) {
	dir := t.TempDir()
	pm := NewPageManager(dir)

	msgs := []*Message{
		newTestMessage("hello", 1),
		newTestMessage("world", 2),
	}

	path, err := pm.Flush("q1", msgs)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}

	loaded, err := pm.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(loaded))
	}
	if string(loaded[0].Payload()) != "hello" {
		t.Fatalf("payload mismatch: %q", loaded[0].Payload())
	}
	if loaded[1].DeliveryTag() != 2 {
		t.Fatalf("delivery tag mismatch: %d", loaded[1].DeliveryTag())
	}

	// Page file should be deleted after load.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("page file not deleted after load")
	}
}

func TestPageManagerCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "pages")
	pm := NewPageManager(dir)
	msgs := []*Message{newTestMessage("x", 1)}
	_, err := pm.Flush("q", msgs)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Fatalf("page dir not created")
	}
}

func newTestMessage(payload string, tag uint64) *Message {
	m := NewMessage([]byte(payload), Properties{ContentType: "text/plain"})
	m.SetDeliveryTag(tag)
	m.SetEnqueuedAt(time.Now())
	return m
}
