// shovel.go defines the Shovel abstraction for moving messages
// from a source to a destination.
package server

// ShovelStatus represents the runtime state of a shovel.
type ShovelStatus int

const (
	ShovelStopped ShovelStatus = iota
	ShovelRunning
	ShovelError
)

// String returns a human-readable status label.
func (s ShovelStatus) String() string {
	switch s {
	case ShovelRunning:
		return "running"
	case ShovelStopped:
		return "stopped"
	case ShovelError:
		return "error"
	default:
		return "unknown"
	}
}

// Shovel continuously copies messages from a source URI to a
// destination URI.
type Shovel struct {
	Name   string
	Source string
	Dest   string
	status ShovelStatus
}

// NewShovel creates a shovel in the Stopped state.
func NewShovel(name, source, dest string) *Shovel {
	return &Shovel{
		Name:   name,
		Source: source,
		Dest:   dest,
		status: ShovelStopped,
	}
}

// Status returns the current shovel status safely.
func (s *Shovel) Status() ShovelStatus { return s.status }

// SetStatus updates the shovel status.
func (s *Shovel) SetStatus(st ShovelStatus) { s.status = st }

// Run is a placeholder stub that simulates a shovel run cycle.
// In production this would establish source and destination
// connections and forward messages.
func (s *Shovel) Run() error {
	s.status = ShovelRunning
	// Placeholder: real implementation would forward messages.
	return nil
}

// Stop transitions the shovel to the Stopped state.
func (s *Shovel) Stop() {
	s.status = ShovelStopped
}
