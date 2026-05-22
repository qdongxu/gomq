// tls.go implements TLS configuration loading for encrypted AMQP
// connections. It supports standard TLS, optional cipher suite
// selection, and mutual TLS (mTLS).
package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig wraps crypto/tls.Config with construction helpers.
type TLSConfig struct {
	config *tls.Config
}

// NewTLSConfig builds a TLSConfig from certificate and key files.
// If caFile is provided, the CA pool is loaded for client verification.
func NewTLSConfig(certFile, keyFile, caFile string, verifyClient bool) (
	*TLSConfig, error,
) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load cert/key: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if caFile != "" {
		pool, err := loadCertPool(caFile)
		if err != nil {
			return nil, fmt.Errorf("load CA: %w", err)
		}
		tlsCfg.ClientCAs = pool
	}

	if verifyClient {
		if caFile == "" {
			return nil, fmt.Errorf(
				"verify_client requires ca_file")
		}
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return &TLSConfig{config: tlsCfg}, nil
}

// SetCipherSuites overrides the default cipher suites.
func (t *TLSConfig) SetCipherSuites(names []string) error {
	suites := make([]uint16, 0, len(names))
	for _, n := range names {
		id, ok := cipherSuiteMap[n]
		if !ok {
			return fmt.Errorf("unknown cipher suite: %s", n)
		}
		suites = append(suites, id)
	}
	t.config.CipherSuites = suites
	return nil
}

// Config returns the underlying *tls.Config.
func (t *TLSConfig) Config() *tls.Config { return t.config }

// loadCertPool reads PEM-encoded certificates from path.
func loadCertPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("invalid CA PEM: %s", path)
	}
	return pool, nil
}

// cipherSuiteMap maps human-readable names to Go tls constants.
var cipherSuiteMap = map[string]uint16{
	"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256":
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384":
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256":
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384":
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	"TLS_RSA_WITH_AES_128_GCM_SHA256":
		tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
	"TLS_RSA_WITH_AES_256_GCM_SHA384":
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
}
