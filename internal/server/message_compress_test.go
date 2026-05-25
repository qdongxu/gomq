// message_compress_test.go tests payload compression.
package server

import (
	"bytes"
	"testing"
)

func TestCompressorThresholdDisabled(t *testing.T) {
	c := NewCompressor(0)
	data := []byte("hello world")
	out, err := c.Compress(data)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if !bytes.Equal(out, data) {
		t.Fatalf("expected unchanged, got %q", out)
	}
}

func TestCompressorBelowThreshold(t *testing.T) {
	c := NewCompressor(100)
	data := []byte("short")
	out, err := c.Compress(data)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if !bytes.Equal(out, data) {
		t.Fatalf("expected unchanged, got %q", out)
	}
}

func TestCompressorRoundTrip(t *testing.T) {
	c := NewCompressor(10)
	data := bytes.Repeat([]byte("a"), 100)
	compressed, err := c.Compress(data)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if len(compressed) >= len(data) {
		t.Fatalf("compression ineffective: %d >= %d", len(compressed), len(data))
	}
	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(decompressed, data) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestCompressorDecompressInvalid(t *testing.T) {
	c := NewCompressor(10)
	_, err := c.Decompress([]byte("not compressed"))
	if err == nil {
		t.Fatalf("expected error for invalid data")
	}
}
