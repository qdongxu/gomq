// federation.go defines the federation configuration for cross-node
// exchange and queue routing.
package server

// AckMode defines how a federation link acknowledges messages.
type AckMode string

const (
	AckOnConfirm  AckMode = "on-confirm"
	AckOnPublish AckMode = "on-publish"
	AckNoAck     AckMode = "no-ack"
)

// FederationConfig holds parameters for a federation link.
type FederationConfig struct {
	Name           string   // unique identifier for this federation
	Upstreams      []string // upstream node URLs
	Exchange       string   // target exchange
	Queue          string   // target queue
	RoutingKey     string   // routing key
	ReconnectDelay int      // seconds between reconnection attempts
	PrefetchCount  int      // prefetch window size
	AckMode        AckMode  // acknowledgement mode
}

// NewFederationConfig creates a federation config with defaults.
func NewFederationConfig(name string) *FederationConfig {
	return &FederationConfig{
		Name:           name,
		Upstreams:      []string{},
		ReconnectDelay: 5,
		PrefetchCount:  10,
		AckMode:        AckOnConfirm,
	}
}
