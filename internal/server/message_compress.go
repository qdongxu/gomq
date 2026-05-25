// message_compress.go provides optional payload compression for messages.
// When enabled, messages whose payload exceeds a size threshold are
// transparently compressed on enqueue and decompressed on dequeue.
package server

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
)

// Compressor handles payload compression and decompression.
type Compressor struct {
	threshold int // minimum payload size to trigger compression
}

// NewCompressor creates a compressor with the given threshold in bytes.
// A threshold of 0 disables compression.
func NewCompressor(threshold int) *Compressor {
	return &Compressor{threshold: threshold}
}

// Compress returns a compressed copy of data if it exceeds the threshold.
// Otherwise it returns the original data unchanged.
func (c *Compressor) Compress(data []byte) ([]byte, error) {
	if c.threshold <= 0 || len(data) < c.threshold {
		return data, nil
	}
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, zlib.BestSpeed)
	if err != nil {
		return nil, fmt.Errorf("compressor init: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("compress: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("compress close: %w", err)
	}
	return buf.Bytes(), nil
}

// Decompress restores the original payload from compressed data.
func (c *Compressor) Decompress(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("decompress init: %w", err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("decompress: %w", err)
	}
	return out, nil
}
