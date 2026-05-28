// flusher_test.go — unit tests for the unified flush scheduler.
package server

import (
	"testing"
	"time"
)

func TestFlushScheduler_RegisterUnregister(t *testing.T) {
	fs := NewFlushScheduler(10 * time.Millisecond)
	defer fs.Stop()

	ch := NewChannel(1, nil)
	fs.Register(1, ch)

	// Unregister should not panic.
	fs.Unregister(1)
}

func TestFlushScheduler_Stop(t *testing.T) {
	fs := NewFlushScheduler(50 * time.Millisecond)
	// Stop should not panic or deadlock.
	fs.Stop()
}
