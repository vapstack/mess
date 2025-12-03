package main

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/vapstack/mess/internal/storage"
)

func (n *node) loadCert() error {

	caBytes, err := storage.ReadFile(filepath.Join(n.path, "mess.crt"))
	if err != nil {
		return fmt.Errorf("reading mess.crt: %v", err)
	}

	n.pool = x509.NewCertPool()
	if ok := n.pool.AppendCertsFromPEM(caBytes); !ok {
		return fmt.Errorf("failed to append certificate to the pool")
	}

	block, _ := pem.Decode(caBytes)
	if block == nil {
		return fmt.Errorf("no PEM data in mess.crt")
	}

	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse root certificate: %w", err)
	}

	pub, ok := caCert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return fmt.Errorf("root public key is not ed25519")
	}
	n.pubk = pub

	keyBytes, err := storage.ReadFile(filepath.Join(n.path, "node.key"))
	if err != nil {
		return fmt.Errorf("reading node.key: %w", err)
	}

	crtBytes, err := storage.ReadFile(filepath.Join(n.path, "node.crt"))
	if err != nil {
		return fmt.Errorf("reading node.crt: %w", err)
	}

	block, _ = pem.Decode(crtBytes)
	if block == nil {
		return fmt.Errorf("no PEM data in node.crt")
	}

	nodeCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse node certificate: %w", err)
	}

	nodeID, err := strconv.ParseUint(nodeCert.Subject.CommonName, 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing certificate node id: %w", err)
	}
	if n.id != nodeID {
		return fmt.Errorf("node certificate contains a wrong node id")
	}

	cert, err := tls.X509KeyPair(crtBytes, keyBytes)
	if err != nil {
		return fmt.Errorf("construct cert: %w", err)
	}

	n.cert.Store(&cert)

	return nil
}
