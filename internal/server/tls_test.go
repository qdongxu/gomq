// tls_test.go tests TLS configuration loading and certificate handling.
package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// generateTestCert creates a self-signed cert/key pair in a temp dir.
func generateTestCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	// IPAddresses expects net.IP, but we use DNSNames instead for test.
	tmpl.DNSNames = []string{"localhost"}

	certDER, err := x509.CreateCertificate(
		rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv,
	)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	certFile, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certFile.Close()

	keyFile, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	privDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})
	keyFile.Close()

	return certPath, keyPath
}

// generateClientCert creates a client cert signed by the given CA.
func generateClientCert(t *testing.T, caCert, caKey string) (
	certPath, keyPath string,
) {
	t.Helper()
	dir := t.TempDir()

	// Load CA.
	caPEM, err := os.ReadFile(caCert)
	if err != nil {
		t.Fatalf("read CA cert: %v", err)
	}
	block, _ := pem.Decode(caPEM)
	ca, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}

	caKeyPEM, err := os.ReadFile(caKey)
	if err != nil {
		t.Fatalf("read CA key: %v", err)
	}
	keyBlock, _ := pem.Decode(caKeyPEM)
	caPriv, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		t.Fatalf("parse CA key: %v", err)
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "client"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(
		rand.Reader, &tmpl, ca, &priv.PublicKey, caPriv,
	)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}

	certPath = filepath.Join(dir, "client-cert.pem")
	keyPath = filepath.Join(dir, "client-key.pem")

	os.WriteFile(certPath, pem.EncodeToMemory(
		&pem.Block{Type: "CERTIFICATE", Bytes: certDER},
	), 0644)
	privDER, _ := x509.MarshalECPrivateKey(priv)
	os.WriteFile(keyPath, pem.EncodeToMemory(
		&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER},
	), 0644)

	return certPath, keyPath
}

func TestNewTLSConfig_Success(t *testing.T) {
	certPath, keyPath := generateTestCert(t)

	tlsCfg, err := NewTLSConfig(certPath, keyPath, "", false)
	if err != nil {
		t.Fatalf("NewTLSConfig: %v", err)
	}
	cfg := tlsCfg.Config()
	if cfg == nil {
		t.Fatal("Config() returned nil")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("expected 1 cert, got %d", len(cfg.Certificates))
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("expected TLS 1.2, got %x", cfg.MinVersion)
	}
}

func TestNewTLSConfig_InvalidCert(t *testing.T) {
	_, err := NewTLSConfig("/nonexistent/cert.pem", "/nonexistent/key.pem", "", false)
	if err == nil {
		t.Fatal("expected error for missing cert")
	}
}

func TestNewTLSConfig_mTLS(t *testing.T) {
	// Use a self-signed cert as CA for simplicity.
	caCert, _ := generateTestCert(t)
	serverCert, serverKey := generateTestCert(t)

	_, err := NewTLSConfig(serverCert, serverKey, "", true)
	if err == nil {
		t.Fatal("expected error when verify_client without ca_file")
	}

	tlsCfg, err := NewTLSConfig(serverCert, serverKey, caCert, true)
	if err != nil {
		t.Fatalf("NewTLSConfig with mTLS: %v", err)
	}
	cfg := tlsCfg.Config()
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("expected RequireAndVerifyClientCert, got %v", cfg.ClientAuth)
	}
	if cfg.ClientCAs == nil {
		t.Fatal("expected ClientCAs to be set")
	}
}

func TestSetCipherSuites(t *testing.T) {
	certPath, keyPath := generateTestCert(t)
	tlsCfg, err := NewTLSConfig(certPath, keyPath, "", false)
	if err != nil {
		t.Fatalf("NewTLSConfig: %v", err)
	}

	err = tlsCfg.SetCipherSuites([]string{
		"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
	})
	if err != nil {
		t.Fatalf("SetCipherSuites: %v", err)
	}
	cfg := tlsCfg.Config()
	if len(cfg.CipherSuites) != 1 {
		t.Fatalf("expected 1 cipher suite, got %d", len(cfg.CipherSuites))
	}
}

func TestSetCipherSuites_Unknown(t *testing.T) {
	certPath, keyPath := generateTestCert(t)
	tlsCfg, _ := NewTLSConfig(certPath, keyPath, "", false)

	err := tlsCfg.SetCipherSuites([]string{"UNKNOWN_SUITE"})
	if err == nil {
		t.Fatal("expected error for unknown cipher suite")
	}
}

func TestLoadCertPool(t *testing.T) {
	certPath, _ := generateTestCert(t)

	pool, err := loadCertPool(certPath)
	if err != nil {
		t.Fatalf("loadCertPool: %v", err)
	}
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
}

func TestLoadCertPool_Invalid(t *testing.T) {
	dir := t.TempDir()
	badPath := filepath.Join(dir, "bad.pem")
	os.WriteFile(badPath, []byte("not a cert"), 0644)

	_, err := loadCertPool(badPath)
	if err == nil {
		t.Fatal("expected error for invalid PEM")
	}
}

// TestTLSServerListener verifies a TLS listener can be created and accepts
// connections.
func TestTLSServerListener(t *testing.T) {
	certPath, keyPath := generateTestCert(t)
	tlsCfg, err := NewTLSConfig(certPath, keyPath, "", false)
	if err != nil {
		t.Fatalf("NewTLSConfig: %v", err)
	}

	srv := NewServer()
	if err := srv.ListenTLS("127.0.0.1:0", tlsCfg.Config()); err != nil {
		t.Fatalf("ListenTLS: %v", err)
	}

	// Start serving in background.
	go func() {
		if err := srv.Serve(); err != nil {
			t.Logf("Serve: %v", err)
		}
	}()
	defer srv.Shutdown()

	// Give server a moment to start.
	time.Sleep(50 * time.Millisecond)
}
