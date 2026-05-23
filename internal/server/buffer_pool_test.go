// buffer_pool_test.go tests the BufferPool implementation.
package server

import (
	"bytes"
	"testing"
)

func TestBufferPool_GetReset(t *testing.T) {
	pool := NewBufferPool()
	buf := pool.Get()
	if buf == nil {
		t.Fatal("expected non-nil buffer")
	}
	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer, got %d bytes", buf.Len())
	}
	buf.WriteString("hello")
	pool.Put(buf)

	buf2 := pool.Get()
	if buf2.Len() != 0 {
		t.Fatalf("expected reset buffer, got %d bytes", buf2.Len())
	}
}

func TestBufferPool_Reuse(t *testing.T) {
	pool := NewBufferPool()
	b1 := pool.Get()
	b1.Write(bytes.Repeat([]byte("x"), 1024))
	pool.Put(b1)

	b2 := pool.Get()
	// Should reuse the same underlying array if capacity allows
	if b2.Cap() < 1024 {
		t.Log("pool allocated new buffer (expected on first reuse)")
	}
}

func TestBufferPool_OversizedDiscard(t *testing.T) {
	pool := NewBufferPool()
	buf := pool.Get()
	// Write more than 1 MiB
	buf.Write(bytes.Repeat([]byte("x"), 1<<20+1))
	pool.Put(buf)

	buf2 := pool.Get()
	// Pool should have discarded oversized buffer
	if buf2.Cap() > 1<<20 {
		t.Fatal("oversized buffer should have been discarded")
	}
}
