// external.go implements the EXTERNAL SASL mechanism.
// EXTERNAL derives the user identity from the underlying transport
// security layer, typically a TLS client certificate.
package auth

import (
	"fmt"
	"net"
)

// EXTERNAL is a SASL mechanism that delegates identity to the
// transport (e.g. TLS peer certificate).
type EXTERNAL struct{}

// NewEXTERNAL creates an EXTERNAL mechanism.
func NewEXTERNAL() *EXTERNAL { return &EXTERNAL{} }

// Name returns "EXTERNAL".
func (e *EXTERNAL) Name() string { return "EXTERNAL" }

// Challenge returns nil (EXTERNAL is not challenge-response).
func (e *EXTERNAL) Challenge() ([]byte, error) { return nil, nil }

// Response extracts the username from the TLS peer certificate CN.
// The client response is ignored for EXTERNAL.
func (e *EXTERNAL) Response(conn net.Conn, _ []byte) (string, error) {
	cn, err := PeerCertificateCN(conn)
	if err != nil {
		return "", fmt.Errorf("EXTERNAL: %w", err)
	}
	if cn == "" {
		return "", fmt.Errorf("EXTERNAL: empty certificate CN")
	}
	return cn, nil
}
