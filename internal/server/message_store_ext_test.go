// message_store_ext_test.go tests ExtendedMessageStore.
package server

import (
	"bytes"
	"os"
	"testing"
)

func TestExtendedStoreCompression(t *testing.T) {
	base := NewMessageStore()
	opts := StoreOptions{CompressionThreshold: 10}
	es := NewExtendedMessageStore(base, opts)

	payload := bytes.Repeat([]byte("a"), 100)
	msg := newTestMessage(string(payload), 1)

	if err := es.EnqueueExt("q", msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	out, ok, err := es.DequeueExt("q")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if !ok {
		t.Fatalf("expected message")
	}
	if !bytes.Equal(out.Payload(), payload) {
		t.Fatalf("payload mismatch after round-trip")
	}
}

func TestExtendedStorePaging(t *testing.T) {
	dir := t.TempDir()
	base := NewMessageStore()
	opts := StoreOptions{
		CompressionThreshold: 0,
		MaxInMemoryMessages:  4,
		PageDir:              dir,
	}
	es := NewExtendedMessageStore(base, opts)

	for i := 1; i <= 6; i++ {
		msg := newTestMessage("msg", uint64(i))
		if err := es.EnqueueExt("q", msg); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	// After paging, memory should have <= 4 messages.
	if es.Len("q") > 4 {
		t.Fatalf("expected <=4 in memory, got %d", es.Len("q"))
	}
}

func TestExtendedStoreNoPagingWhenDisabled(t *testing.T) {
	base := NewMessageStore()
	opts := StoreOptions{CompressionThreshold: 0, MaxInMemoryMessages: 0}
	es := NewExtendedMessageStore(base, opts)

	for i := 1; i <= 100; i++ {
		msg := newTestMessage("x", uint64(i))
		if err := es.EnqueueExt("q", msg); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	if es.Len("q") != 100 {
		t.Fatalf("expected 100 in memory, got %d", es.Len("q"))
	}
}

func TestExtendedStoreSmallPayloadNoCompress(t *testing.T) {
	base := NewMessageStore()
	opts := StoreOptions{CompressionThreshold: 100}
	es := NewExtendedMessageStore(base, opts)

	msg := newTestMessage("tiny", 1)
	if err := es.EnqueueExt("q", msg); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	out, ok, err := es.DequeueExt("q")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if !ok {
		t.Fatalf("expected message")
	}
	if string(out.Payload()) != "tiny" {
		t.Fatalf("payload mismatch")
	}
}

func TestExtendedStoreSetOptions(t *testing.T) {
	base := NewMessageStore()
	opts := StoreOptions{CompressionThreshold: 0, MaxInMemoryMessages: 0}
	es := NewExtendedMessageStore(base, opts)

	// Initially disabled.
	if es.compressor != nil || es.pager != nil {
		t.Fatalf("expected nil compressor and pager")
	}

	// Enable at runtime.
	dir := t.TempDir()
	es.SetOptions(StoreOptions{
		CompressionThreshold: 10,
		MaxInMemoryMessages:  4,
		PageDir:              dir,
	})
	if es.compressor == nil || es.pager == nil {
		t.Fatalf("expected non-nil after SetOptions")
	}
}

func TestExtendedStoreLoadPage(t *testing.T) {
	dir := t.TempDir()
	base := NewMessageStore()
	opts := StoreOptions{
		CompressionThreshold: 0,
		MaxInMemoryMessages:  2,
		PageDir:              dir,
	}
	es := NewExtendedMessageStore(base, opts)

	for i := 1; i <= 4; i++ {
		msg := newTestMessage("m", uint64(i))
		if err := es.EnqueueExt("q", msg); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	// Find the page file.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected page file")
	}

	// Load all page files.
	for _, entry := range entries {
		pagePath := dir + "/" + entry.Name()
		if err := es.LoadPage("q", pagePath); err != nil {
			t.Fatalf("load page %s: %v", pagePath, err)
		}
	}

	// All 4 messages should be in memory now.
	if es.Len("q") != 4 {
		t.Fatalf("expected 4 after load, got %d", es.Len("q"))
	}
}
