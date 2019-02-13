package cert

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"github.com/pkg/errors"
)

// Info captures TLS certificate information about a certain public/private
// keypair and an optional CA certificate.
//
// Given support for PKI setups, these two bits of information are
// normally used and passed around together, so this structure helps with that.
type Info struct {
	keypair tls.Certificate
	ca      *x509.Certificate
}

// NewInfo creates a new cert.Info with sane defaults.
func NewInfo(keypair tls.Certificate, ca *x509.Certificate) *Info {
	return &Info{
		keypair: keypair,
		ca:      ca,
	}
}

// KeyPair returns the public/private key pair.
func (c *Info) KeyPair() tls.Certificate {
	return c.keypair
}

// CA returns the CA certificate.
func (c *Info) CA() *x509.Certificate {
	return c.ca
}

// PublicKey is a convenience to encode the underlying public key to ASCII.
func (c *Info) PublicKey() []byte {
	data := c.KeyPair().Certificate[0]
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: data})
}

// PrivateKey is a convenience to encode the underlying private key.
func (c *Info) PrivateKey() []byte {
	key, ok := c.KeyPair().PrivateKey.(*rsa.PrivateKey)
	if !ok {
		return nil
	}
	data := x509.MarshalPKCS1PrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: data})
}

// Fingerprint returns the fingerprint of the public key.
func (c *Info) Fingerprint() string {
	fingerprint, err := FingerprintStr(string(c.PublicKey()))
	// Parsing should never fail, since we generated the cert ourselves,
	// but let's check the error for good measure.
	if err != nil {
		panic("invalid public key material")
	}
	return fingerprint
}

// Fingerprint returns the fingerprint of the certificate
func Fingerprint(cert *x509.Certificate) string {
	return fmt.Sprintf("%x", sha256.Sum256(cert.Raw))
}

// FingerprintStr returns the fingerprint of the certificate as a string
func FingerprintStr(c string) (string, error) {
	pemCertificate, _ := pem.Decode([]byte(c))
	if pemCertificate == nil {
		return "", errors.Errorf("invalid certificate")
	}

	cert, err := x509.ParseCertificate(pemCertificate.Bytes)
	if err != nil {
		return "", err
	}

	return Fingerprint(cert), nil
}
