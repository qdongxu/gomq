// backpressure.go provides memory-based backpressure control.
// When memory usage exceeds a threshold, new connections and publishes
// are throttled and Channel.Flow is triggered.
package server

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// BackPressure controls admission based on memory pressure.
type BackPressure struct {
	thresholdPercent float64 // 0-100
	enabled          bool
	paused           atomic.Bool
	memoryUsage      atomic.Int64 // simulated bytes (for tests)
	mu               sync.RWMutex
}

// NewBackPressure creates a backpressure controller.
// thresholdPercent is the memory-usage percentage that triggers throttling.
// 0 disables backpressure.
func NewBackPressure(thresholdPercent float64) *BackPressure {
	return &BackPressure{
		thresholdPercent: thresholdPercent,
		enabled:          thresholdPercent > 0,
	}
}

// CanAccept reports whether the broker can accept a new connection
// or message.  Returns false when under memory pressure.
func (bp *BackPressure) CanAccept() bool {
	if !bp.enabled {
		return true
	}
	return !bp.paused.Load()
}

// Check samples current memory and updates the paused state.
// Returns true if the broker is now under pressure.
func (bp *BackPressure) Check() bool {
	if !bp.enabled {
		return false
	}
	usage := bp.currentMemoryPercent()
	underPressure := usage >= bp.thresholdPercent
	bp.paused.Store(underPressure)
	return underPressure
}

// currentMemoryPercent returns the current memory usage as a percentage
// of total available RAM.
func (bp *BackPressure) currentMemoryPercent() float64 {
	// Prefer simulated value for deterministic tests.
	if sim := bp.memoryUsage.Load(); sim > 0 {
		// Treat simulated value as a percentage directly.
		return float64(sim)
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// HeapAlloc as percentage of Sys (total memory obtained from OS).
	if m.Sys == 0 {
		return 0
	}
	return float64(m.HeapAlloc) / float64(m.Sys) * 100
}

// SetThreshold updates the threshold at runtime.
func (bp *BackPressure) SetThreshold(percent float64) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.thresholdPercent = percent
	bp.enabled = percent > 0
}

// SetSimulatedUsage sets a fake memory percentage for testing.
func (bp *BackPressure) SetSimulatedUsage(percent int64) {
	bp.memoryUsage.Store(percent)
}
