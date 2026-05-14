// authenticator.go defines the authentication interface and a simple
// in-memory implementation.
package server

// Authenticator validates client credentials.
type Authenticator interface {
	Authenticate(username, password string) error
}

// MemoryAuthenticator stores users in a map.
type MemoryAuthenticator struct {
	users map[string]string // username → password
}

// NewMemoryAuthenticator creates an authenticator with the default
// guest/guest account.
func NewMemoryAuthenticator() *MemoryAuthenticator {
	return &MemoryAuthenticator{
		users: map[string]string{
			"guest": "guest",
		},
	}
}

// Authenticate checks username and password against the memory map.
func (a *MemoryAuthenticator) Authenticate(
	username, password string,
) error {
	if expect, ok := a.users[username]; ok && expect == password {
		return nil
	}
	return ErrAuthFailed
}
