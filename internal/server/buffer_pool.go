// buffer_pool.go implements a sync.Pool-backed buffer pool to reduce
// GC pressure from frequent bytes.Buffer allocations.
package server

import (
	"bytes"
	"sync"
)

// BufferPool holds reusable bytes.Buffer instances via sync.Pool.
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool creates a buffer pool with an initial reset function.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &bytes.Buffer{}
			},
		},
	}
}

// Get retrieves a buffer from the pool. The buffer is reset before
// being returned, so it is safe to use immediately.
func (p *BufferPool) Get() *bytes.Buffer {
	buf := p.pool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// Put returns a buffer to the pool. Callers must not use the buffer
// after returning it.
func (p *BufferPool) Put(buf *bytes.Buffer) {
	// Avoid holding references to large underlying arrays.
	if buf.Cap() > 1<<20 { // 1 MiB
		return // let GC reclaim oversized buffers
	}
	p.pool.Put(buf)
}
