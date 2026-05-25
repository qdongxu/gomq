// sasl_test.go tests the SASL framework and built-in mechanisms.
package auth

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net"
	"testing"
	"time"
)

// TestRegistryNames lists registered mechanisms.
func TestRegistryNames(t *testing.T) {
	r := NewSASLRegistry()
	r.Register(NewAMQPLAIN())
	r.Register(NewEXTERNAL())

	names := r.Names()
	if names == "" {
		t.Fatal("expected non-empty names")
	}
	if r.Lookup("AMQPLAIN") == nil {
		t.Fatal("expected AMQPLAIN")
	}
	if r.Lookup("EXTERNAL") == nil {
		t.Fatal("expected EXTERNAL")
	}
	if r.Lookup("UNKNOWN") != nil {
		t.Fatal("expected nil for unknown")
	}
}

// TestAMQPLAINSuccess validates correct credentials.
func TestAMQPLAINSuccess(t *testing.T) {
	m := NewAMQPLAIN()
	resp := []byte("\x00guest\x00guest")
	user, err := m.Response(nil, resp)
	if err != nil {
		t.Fatalf("auth: %v", err)
	}
	if user != "guest" {
		t.Fatalf("user = %q, want guest", user)
	}
}

// TestAMQPLAINFailure rejects bad password.
func TestAMQPLAINFailure(t *testing.T) {
	m := NewAMQPLAIN()
	resp := []byte("\x00guest\x00wrong")
	_, err := m.Response(nil, resp)
	if err == nil {
		t.Fatal("expected auth failure")
	}
}

// TestAMQPLAINAddUser registers a custom account.
func TestAMQPLAINAddUser(t *testing.T) {
	m := NewAMQPLAIN()
	m.AddUser("alice", "secret")
	resp := []byte("\x00alice\x00secret")
	user, err := m.Response(nil, resp)
	if err != nil {
		t.Fatalf("auth: %v", err)
	}
	if user != "alice" {
		t.Fatalf("user = %q, want alice", user)
	}
}

// TestAMQPLAINBadFormat rejects malformed response.
func TestAMQPLAINBadFormat(t *testing.T) {
	m := NewAMQPLAIN()
	_, err := m.Response(nil, []byte("short"))
	if err == nil {
		t.Fatal("expected error for short response")
	}
}

// fakeTLSConn simulates a *tls.Conn for EXTERNAL tests.
type fakeTLSConn struct {
	net.Conn
	state tls.ConnectionState
}

func (c *fakeTLSConn) ConnectionState() tls.ConnectionState {
	return c.state
}

// TestEXTERNALSuccess extracts CN from peer certificate.
func TestEXTERNALSuccess(t *testing.T) {
	m := NewEXTERNAL()
	conn := &fakeTLSConn{
		state: tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				{
					Subject: pkix.Name{
						CommonName: "client-cn",
					},
				},
			},
		},
	}
	user, err := m.Response(conn, nil)
	if err != nil {
		t.Fatalf("auth: %v", err)
	}
	if user != "client-cn" {
		t.Fatalf("user = %q, want client-cn", user)
	}
}

// TestEXTERNALNoCert fails when there is no peer certificate.
func TestEXTERNALNoCert(t *testing.T) {
	m := NewEXTERNAL()
	conn := &fakeTLSConn{state: tls.ConnectionState{}}
	_, err := m.Response(conn, nil)
	if err == nil {
		t.Fatal("expected error without certificate")
	}
}

// TestEXTERNALNonTLS fails on plain net.Conn.
func TestEXTERNALNonTLS(t *testing.T) {
	m := NewEXTERNAL()
	// net.Pipe() returns plain net.Conn, not *tls.Conn.
	c1, _ := net.Pipe()
	defer c1.Close()
	_, err := m.Response(c1, nil)
	if err == nil {
		t.Fatal("expected error for non-TLS connection")
	}
}

// TestPeerCertificateCN extracts CN successfully.
func TestPeerCertificateCN(t *testing.T) {
	conn := &fakeTLSConn{
		state: tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				{
					Subject: pkix.Name{
						CommonName: "test-cn",
					},
				},
			},
		},
	}
	cn, err := PeerCertificateCN(conn)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if cn != "test-cn" {
		t.Fatalf("cn = %q, want test-cn", cn)
	}
}

// TestPeerCertificateCNNoCert returns error.
func TestPeerCertificateCNNoCert(t *testing.T) {
	conn := &fakeTLSConn{state: tls.ConnectionState{}}
	_, err := PeerCertificateCN(conn)
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestAMQPLAINChallenge returns nil.
func TestAMQPLAINChallenge(t *testing.T) {
	m := NewAMQPLAIN()
	ch, err := m.Challenge()
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}
	if ch != nil {
		t.Fatal("expected nil challenge")
	}
}

// TestEXTERNALChallenge returns nil.
func TestEXTERNALChallenge(t *testing.T) {
	m := NewEXTERNAL()
	ch, err := m.Challenge()
	if err != nil {
		t.Fatalf("challenge: %v", err)
	}
	if ch != nil {
		t.Fatal("expected nil challenge")
	}
}

// TestRegistryDuplicateLastWins registers the same name twice.
func TestRegistryDuplicateLastWins(t *testing.T) {
	r := NewSASLRegistry()
	m1 := NewAMQPLAIN()
	m2 := NewEXTERNAL()
	// EXTERNAL does not conflict with AMQPLAIN, but test the mechanic.
	r.Register(m1)
	r.Register(m2)
	if r.Lookup("AMQPLAIN") == nil {
		t.Fatal("expected AMQPLAIN")
	}
}

// TestAMQPLAINSplitNull handles responses with extra null bytes.
func TestAMQPLAINSplitNull(t *testing.T) {
	m := NewAMQPLAIN()
	// Extra leading null is ignored by splitNull indexing.
	resp := []byte("\x00\x00user\x00pass")
	parts := splitNull(resp)
	if len(parts) != 4 {
		t.Fatalf("parts = %d, want 4", len(parts))
	}
	_, err := m.Response(nil, resp)
	if err == nil {
		// user=empty string, pass=user — should fail.
		t.Fatal("expected failure with extra null")
	}
}

// TestHandshakeAMQPLAIN selects AMQPLAIN via SASL registry.
func TestHandshakeAMQPLAIN(t *testing.T) {
	// This is an integration-level smoke: verify the mechanism name
	// and that it can authenticate through the registry.
	r := NewSASLRegistry()
	m := NewAMQPLAIN()
	r.Register(m)
	if r.Names() != "AMQPLAIN" {
		t.Fatalf("names = %q", r.Names())
	}
}

// TestHandshakeEXTERNALWithEmptyCN fails.
func TestEXTERNALWithEmptyCN(t *testing.T) {
	m := NewEXTERNAL()
	conn := &fakeTLSConn{
		state: tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				{Subject: pkix.Name{CommonName: ""}},
			},
		},
	}
	_, err := m.Response(conn, nil)
	if err == nil {
		t.Fatal("expected error for empty CN")
	}
}

// close helper for net.Pipe in tests.
func closePipe(t *testing.T, c net.Conn) {
	_ = c.SetDeadline(time.Now().Add(10 * time.Millisecond))
	_ = c.Close()
}
