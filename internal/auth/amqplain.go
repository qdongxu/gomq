// amqplain.go implements the AMQPLAIN SASL mechanism.
// AMQPLAIN is functionally identical to PLAIN but historically
// used by some RabbitMQ clients.
package auth

import (
	"fmt"
	"net"
)

// AMQPLAIN stores username/password pairs for authentication.
type AMQPLAIN struct {
	users map[string]string // username → password
}

// NewAMQPLAIN creates an AMQPLAIN mechanism with the default guest
// account.
func NewAMQPLAIN() *AMQPLAIN {
	return &AMQPLAIN{
		users: map[string]string{
			"guest": "guest",
		},
	}
}

// Name returns the SASL mechanism name.
func (a *AMQPLAIN) Name() string { return "AMQPLAIN" }

// Challenge returns nil (AMQPLAIN is not challenge-response).
func (a *AMQPLAIN) Challenge() ([]byte, error) { return nil, nil }

// Response validates a PLAIN-style response: \x00user\x00pass.
func (a *AMQPLAIN) Response(_ net.Conn, response []byte) (string, error) {
	if len(response) < 3 {
		return "", fmt.Errorf("invalid response length")
	}
	parts := splitNull(response)
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid response format")
	}
	user, pass := string(parts[1]), string(parts[2])
	if expect, ok := a.users[user]; !ok || expect != pass {
		return "", fmt.Errorf("authentication failed")
	}
	return user, nil
}

// AddUser registers a new username/password pair.
func (a *AMQPLAIN) AddUser(user, pass string) {
	a.users[user] = pass
}

// splitNull divides a byte slice by null bytes.
func splitNull(b []byte) [][]byte {
	var parts [][]byte
	start := 0
	for i, c := range b {
		if c == 0 {
			parts = append(parts, b[start:i])
			start = i + 1
		}
	}
	parts = append(parts, b[start:])
	return parts
}
