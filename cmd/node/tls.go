package main

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"time"
	"unsafe"

	"github.com/vapstack/mess"
	"github.com/vapstack/mess/internal"
)

/*
func (n *node) loadMessPublicKey() (ed25519.PublicKey, error) {
	caBytes, err := internal.ReadFile(filepath.Join(n.path, "mess.crt"))
	if err != nil {
		return nil, fmt.Errorf("reading mess.crt: %v", e)
	}
	cert, err := x509.ParseCertificate(caBytes)
	if err != nil {
		return nil, fmt.Errorf("parse cert: %w", err)
	}
	return cert.PublicKey.(ed25519.PublicKey), nil
}
*/

func (n *node) loadCert() error {

	caBytes, err := internal.ReadFile(filepath.Join(n.path, "mess.crt"))
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

	keyBytes, err := internal.ReadFile(filepath.Join(n.path, "node.key"))
	if err != nil {
		return fmt.Errorf("reading node.key: %w", err)
	}
	crtBytes, err := internal.ReadFile(filepath.Join(n.path, "node.crt"))
	if err != nil {
		return fmt.Errorf("reading node.crt: %w", err)
	}

	cert, err := tls.X509KeyPair(crtBytes, keyBytes)
	if err != nil {
		return fmt.Errorf("construct cert: %w", err)
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
	if err != nil {
		return mess.ErrInvalidNode
	}
	if nid == n.id {
		return mess.ErrInvalidNode
	}
	if len(cert.Subject.Organization) == 0 || cert.Subject.Organization[0] != mess.ServiceName {
		return errors.New("invalid org")
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
	if len(cert.Subject.Organization) == 0 || cert.Subject.Organization[0] != mess.ServiceName {
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

func verifySignatureHeader(publicKey ed25519.PublicKey, value string) error {
	data := unsafe.Slice(unsafe.StringData(value), len(value))
	if len(data) <= 8 {
		return errors.New("invalid data len")
	}

	if !ed25519.Verify(publicKey, data[:8], data[8:]) {
		return errors.New("signature verification failed")
	}

	ts := int64(binary.BigEndian.Uint64(data[:8]))

	if time.Since(time.Unix(ts, 0)) > 10*time.Second {
		return errors.New("signature verification failed")
	}

	return nil
}
