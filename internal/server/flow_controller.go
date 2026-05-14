// flow_controller.go manages channel and connection level flow control.
package server

import "sync"

// FlowController tracks which channels are paused by the server.
type FlowController struct {
	paused     map[uint16]bool
	connPaused bool
	mu         sync.RWMutex
}

// NewFlowController creates a flow controller.
func NewFlowController() *FlowController {
	return &FlowController{
		paused: make(map[uint16]bool),
	}
}

// PauseChannel stops delivery for the given channel.
func (fc *FlowController) PauseChannel(channelID uint16) {
	fc.mu.Lock()
	fc.paused[channelID] = true
	fc.mu.Unlock()
}

// ResumeChannel resumes delivery for the given channel.
func (fc *FlowController) ResumeChannel(channelID uint16) {
	fc.mu.Lock()
	delete(fc.paused, channelID)
	fc.mu.Unlock()
}

// IsChannelActive reports whether the channel may receive frames.
func (fc *FlowController) IsChannelActive(channelID uint16) bool {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	if fc.connPaused {
		return false
	}
	return !fc.paused[channelID]
}

// PauseConnection pauses all channels.
func (fc *FlowController) PauseConnection() {
	fc.mu.Lock()
	fc.connPaused = true
	fc.mu.Unlock()
}

// ResumeConnection resumes all channels.
func (fc *FlowController) ResumeConnection() {
	fc.mu.Lock()
	fc.connPaused = false
	fc.mu.Unlock()
}
