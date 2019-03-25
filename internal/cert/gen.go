package cert

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/user"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/fsys"
)

const defaultCertValidPeriod = 10 * 365 * 24 * time.Hour

// OS is a high-level facade for accessing all operating-system
// level functionality that therm uses.
type OS interface {

	// Hostname returns the host name reported by the kernel.
	Hostname() (string, error)

	// HostNames will generate a list of names for which the certificate will be
	// valid.
	// This will include the hostname and ip address
	HostNames() ([]string, error)

	// User returns the current user.
	User() (*user.User, error)
}

// CertGenerator generator attempts to generate certificates and keys
type CertGenerator struct {
	fileSystem   fsys.FileSystem
	clock        clock.Clock
	os           OS
	organization []string
}

// CertKey represents a tuple of Certificates and Keys as a pair.
type CertKey struct {
	Cert, Key []byte
}

// NewCertGenerator creates a new CertGenerator with sane defaults
func NewCertGenerator(organization []string, options ...Option) *CertGenerator {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &CertGenerator{
		fileSystem:   opts.fileSystem,
		clock:        opts.clock,
		os:           opts.os,
		organization: organization,
	}
}

// Generate will create and populate a certificate file and a key file
func (g *CertGenerator) Generate(cert, key string, certType bool) error {
	if err := g.fileSystem.MkdirAll(path.Dir(cert), 0750); err != nil {
		return errors.WithStack(err)
	}
	if err := g.fileSystem.MkdirAll(path.Dir(key), 0750); err != nil {
		return errors.WithStack(err)
	}

	certKey, err := g.GenerateMemCert(certType)
	if err != nil {
		return errors.WithStack(err)
	}

	if err := g.write(func() (fsys.File, error) {
		return g.fileSystem.Create(cert)
	}, cert, certKey.Cert); err != nil {
		return errors.WithStack(err)
	}

	if err := g.write(func() (fsys.File, error) {
		return g.fileSystem.OpenFile(key, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	}, key, certKey.Key); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

// GenerateMemCert creates client or server certificate and key pair,
// returning them as byte arrays in memory.
func (g *CertGenerator) GenerateMemCert(client bool) (CertKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return CertKey{}, errors.Wrap(err, "failed to generate key")
	}

	validFrom := g.clock.Now()
	validTo := validFrom.Add(defaultCertValidPeriod)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return CertKey{}, errors.Wrap(err, "failed to generate serial number")
	}

	hosts, err := g.os.HostNames()
	if err != nil {
		return CertKey{}, errors.Wrap(err, "failed to get host names")
	}

	username := "UNKNOWN"
	userEntry, err := g.os.User()
	if err == nil && userEntry.Username != "" {
		username = userEntry.Username
	}

	hostname, err := g.os.Hostname()
	if err != nil {
		hostname = "UNKNOWN"
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: g.organization,
			CommonName:   fmt.Sprintf("%s@%s", username, hostname),
		},
		NotBefore: validFrom,
		NotAfter:  validTo,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	if client {
		template.ExtKeyUsage = []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
		}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		}
	}

	for _, h := range hosts {
		if ip, _, err := net.ParseCIDR(h); err == nil {
			if !ip.IsLinkLocalUnicast() && !ip.IsLinkLocalMulticast() {
				template.IPAddresses = append(template.IPAddresses, ip)
			}
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	derBytes, err := x509.CreateCertificate(
		rand.Reader,
		&template,
		&template,
		&privateKey.PublicKey,
		privateKey,
	)
	if err != nil {
		return CertKey{}, errors.Wrap(err, "failed to create certificate")
	}

	cert := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: derBytes,
	})
	key := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	return CertKey{
		Cert: cert,
		Key:  key,
	}, nil
}

func (g *CertGenerator) write(fn func() (fsys.File, error), path string, bytes []byte) error {
	out, err := fn()
	defer func() {
		if out != nil {
			out.Close()
		}
	}()
	if err != nil {
		return errors.Wrapf(err, "failed to open %q for writing", path)
	}
	if _, err := out.Write(bytes); err != nil {
		return errors.Wrapf(err, "failed to write to %q", path)
	}
	if err := out.Sync(); err != nil {
		return errors.Wrapf(err, "failed to flush write to %q", path)
	}
	return nil
}
