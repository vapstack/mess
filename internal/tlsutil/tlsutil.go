package tlsutil

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strconv"

	"github.com/vapstack/mess"
)

func PeerCertVerifier(denyNodeID uint64, pool *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	verifyOptions := x509.VerifyOptions{Roots: pool}
	return func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) == 0 || len(raw[0]) == 0 {
			return mess.ErrNoCertProvided
		}
		cert, e := x509.ParseCertificate(raw[0])
		if e != nil {
			return e
		}
		id, err := strconv.ParseUint(cert.Subject.CommonName, 10, 64)
		if err != nil {
			return mess.ErrInvalidNode
		}
		if id == denyNodeID {
			return mess.ErrInvalidNode
		}
		if len(cert.Subject.Organization) == 0 || cert.Subject.Organization[0] != mess.ServiceName {
			return errors.New("invalid org")
		}
		_, err = cert.Verify(verifyOptions)
		return err
	}
}

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
