// channel_manager.go manages channel allocation and lifecycle on a
// single connection.
package server

import (
	"fmt"
	"sync"
)

// ChannelManager tracks open channels for one connection.
type ChannelManager struct {
	channels map[uint16]*Channel
	mu       sync.RWMutex
	maxCh    uint16
}

// NewChannelManager creates a manager with the given channel limit.
func NewChannelManager(maxCh uint16) *ChannelManager {
	if maxCh == 0 {
		maxCh = 2048
	}
	return &ChannelManager{
		channels: make(map[uint16]*Channel),
		maxCh:    maxCh,
	}
}

// Create allocates a new channel with the given ID.
// Returns an error if the ID is already in use or exceeds the limit.
func (m *ChannelManager) Create(
	id uint16,
	conn *Connection,
) (*Channel, error) {
	if id == 0 {
		return nil, fmt.Errorf("channel 0 is reserved")
	}
	if id > m.maxCh {
		return nil, fmt.Errorf("channel %d exceeds max %d", id, m.maxCh)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.channels[id]; exists {
		return nil, fmt.Errorf("channel %d already open", id)
	}
	ch := NewChannel(id, conn)
	ch.Open()
	m.channels[id] = ch
	return ch, nil
}

// Remove deletes a channel from the manager.
func (m *ChannelManager) Remove(id uint16) {
	m.mu.Lock()
	delete(m.channels, id)
	m.mu.Unlock()
}

// Get looks up a channel by ID.
func (m *ChannelManager) Get(id uint16) (*Channel, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ch, ok := m.channels[id]
	return ch, ok
}

// Count returns the number of open channels.
func (m *ChannelManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.channels)
}

// CloseAll closes every channel in the manager.
func (m *ChannelManager) CloseAll() {
	m.mu.Lock()
	chs := make([]*Channel, 0, len(m.channels))
	for _, ch := range m.channels {
		chs = append(chs, ch)
	}
	m.channels = make(map[uint16]*Channel)
	m.mu.Unlock()

	for _, ch := range chs {
		ch.Close()
	}
}
