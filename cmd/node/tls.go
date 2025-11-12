package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"mess"
	"mess/internal"
	"path/filepath"
	"strconv"
)

func (n *node) loadCert() error {

	caBytes, e := internal.ReadFile(filepath.Join(n.path, "mess.crt"))
	if e != nil {
		return fmt.Errorf("reading mess.crt: %v", e)
	}
	n.pool = x509.NewCertPool()
	if ok := n.pool.AppendCertsFromPEM(caBytes); !ok {
		return fmt.Errorf("failed to append certificate to the pool")
	}

	keyBytes, e := internal.ReadFile(filepath.Join(n.path, "node.key"))
	if e != nil {
		return fmt.Errorf("reading node.key: %w", e)
	}
	crtBytes, e := internal.ReadFile(filepath.Join(n.path, "node.crt"))
	if e != nil {
		return fmt.Errorf("reading node.crt: %w", e)
	}

	cert, e := tls.X509KeyPair(crtBytes, keyBytes)
	if e != nil {
		return fmt.Errorf("construct cert: %w", e)
	}

	n.cert.Store(&cert)
	return nil
}

func (n *node) getCert(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	return n.cert.Load(), nil
}

func (n *node) verifyPeerCert(raw [][]byte, _ [][]*x509.Certificate) error {
	if len(raw) == 0 || len(raw[0]) == 0 {
		return mess.ErrNoCertProvided
	}
	cert, e := x509.ParseCertificate(raw[0])
	if e != nil {
		return e
	}
	nid, err := strconv.ParseUint(cert.Subject.CommonName, 10, 64)
	if err != nil || nid == 0 {
		return mess.ErrInvalidNode
	}
	if len(cert.Subject.Organization) == 0 || cert.Subject.Organization[0] != mess.MessService {
		return fmt.Errorf("invalid")
	}
	_, err = cert.Verify(x509.VerifyOptions{Roots: n.pool})
	return err
}

/**/

func VerifyKeyCert(pool *x509.CertPool, keyBytes, crtBytes []byte) error {
	if err := VerifyPK(keyBytes); err != nil {
		return err
	} else if err = VerifyCertCA(pool, crtBytes); err != nil {
		return err
	}
	return nil
}

func VerifyCertCA(pool *x509.CertPool, certPEM []byte) error {
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return fmt.Errorf("invalid PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return err
	}
	if len(cert.Subject.Organization) == 0 || cert.Subject.Organization[0] != mess.MessService {
		return fmt.Errorf("invalid")
	}
	if _, err = cert.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
		return err
	}
	return nil
}

func VerifyPK(keyPEM []byte) error {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return fmt.Errorf("invalid PEM")
	}

	var err error

	switch block.Type {
	case "PRIVATE KEY":
		_, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	case "RSA PRIVATE KEY":
		_, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "EC PRIVATE KEY":
		_, err = x509.ParseECPrivateKey(block.Bytes)
	default:
		return fmt.Errorf("unsupported key type: %v", block.Type)
	}
	if err != nil {
		return err
	}
	return nil
}
