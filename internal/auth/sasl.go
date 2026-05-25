// sasl.go defines the SASL authentication framework for AMQP 0-9-1.
package auth

import (
	"crypto/tls"
	"fmt"
	"net"
)

// Mechanism is a SASL authentication mechanism.
type Mechanism interface {
	// Name returns the SASL mechanism name (e.g. "PLAIN", "EXTERNAL").
	Name() string

	// Challenge issues a server challenge.  For mechanisms that do
	// not use challenge-response (e.g. PLAIN), this returns nil.
	Challenge() ([]byte, error)

	// Response validates a client response and returns the authenticated
	// username or an error.
	Response(conn net.Conn, response []byte) (string, error)
}

// Registry holds all supported SASL mechanisms.
type Registry struct {
	mechs map[string]Mechanism
}

// NewSASLRegistry creates a registry with the built-in mechanisms.
func NewSASLRegistry() *Registry {
	return &Registry{mechs: make(map[string]Mechanism)}
}

// Register adds a mechanism to the registry.
func (r *Registry) Register(m Mechanism) {
	r.mechs[m.Name()] = m
}

// Lookup returns the named mechanism or nil.
func (r *Registry) Lookup(name string) Mechanism {
	return r.mechs[name]
}

// Names returns all registered mechanism names as a space-separated
// string suitable for the AMQP Connection.Start frame.
func (r *Registry) Names() string {
	var names string
	for n := range r.mechs {
		if names != "" {
			names += " "
		}
		names += n
	}
	return names
}

// PeerCertificateCN extracts the CommonName from the peer's TLS
// certificate when the connection supports TLS state.
func PeerCertificateCN(conn net.Conn) (string, error) {
	type tlsConn interface {
		ConnectionState() tls.ConnectionState
	}
	tc, ok := conn.(tlsConn)
	if !ok {
		return "", fmt.Errorf("connection is not TLS")
	}
	state := tc.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return "", fmt.Errorf("no peer certificate")
	}
	return state.PeerCertificates[0].Subject.CommonName, nil
}
