// membership.go tracks cluster members and their health status.
package cluster

import (
	"sync"
	"time"
)

// Status represents a node's health state.
type Status int

const (
	StatusOnline Status = iota
	StatusSuspected
	StatusOffline
)

func (s Status) String() string {
	switch s {
	case StatusOnline:
		return "online"
	case StatusSuspected:
		return "suspected"
	case StatusOffline:
		return "offline"
	}
	return "unknown"
}

// Member extends Node with health status.
type Member struct {
	Node
	Status   Status
	JoinedAt time.Time
}

// Membership tracks all known cluster members.
type Membership struct {
	members map[string]*Member
	mu      sync.RWMutex
}

// NewMembership creates an empty membership tracker.
func NewMembership() *Membership {
	return &Membership{
		members: make(map[string]*Member),
	}
}

// Join adds or updates a member as online.
func (m *Membership) Join(id, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mem, ok := m.members[id]; ok {
		mem.LastSeen = time.Now()
		mem.Status = StatusOnline
		mem.Addr = addr
		return
	}
	m.members[id] = &Member{
		Node:     Node{ID: id, Addr: addr, LastSeen: time.Now()},
		Status:   StatusOnline,
		JoinedAt: time.Now(),
	}
}

// Leave marks a member as offline.
func (m *Membership) Leave(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mem, ok := m.members[id]; ok {
		mem.Status = StatusOffline
	}
}

// Suspect marks a member as suspected (missing heartbeats).
func (m *Membership) Suspect(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mem, ok := m.members[id]; ok {
		mem.Status = StatusSuspected
	}
}

// Members returns a snapshot of all members.
func (m *Membership) Members() []*Member {
	m.mu.RLock()
	out := make([]*Member, 0, len(m.members))
	for _, mem := range m.members {
		out = append(out, mem)
	}
	m.mu.RUnlock()
	return out
}

// OnlineCount returns the number of online members.
func (m *Membership) OnlineCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, mem := range m.members {
		if mem.Status == StatusOnline {
			count++
		}
	}
	return count
}

// Get returns a member by ID.
func (m *Membership) Get(id string) (*Member, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mem, ok := m.members[id]
	return mem, ok
}
