package cert

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// LoadCert reads the server certificate from the given var dir.
//
// If a cluster certificate is found it will be loaded instead.
func LoadCert(dir string, options ...Option) (*Info, error) {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	prefix := "server"
	if opts.fileSystem.Exists(filepath.Join(dir, "cluster.crt")) {
		prefix = "cluster"
	}
	cert, err := keyPairAndCA(dir, prefix, CertServer, opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load TLS certificate")
	}
	return cert, nil
}

// WriteCert writes the given certificate to the correct directory
func WriteCert(dir, prefix string, cert, key []byte, options ...Option) error {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	writeFile := func(suffix string, value []byte) error {
		path := filepath.Join(dir, fmt.Sprintf("%s.%s", prefix, suffix))
		if opts.fileSystem.Exists(path) {
			if err := opts.fileSystem.Remove(path); err != nil {
				return errors.WithStack(err)
			}
		}
		file, err := opts.fileSystem.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return errors.Wrapf(err, "opening %v", suffix)
		}
		if _, err := file.Write(value); err != nil {
			return errors.Wrapf(err, "writing %v", suffix)
		}
		return file.Sync()
	}
	if err := writeFile("crt", cert); err != nil {
		return errors.WithStack(err)
	}
	if err := writeFile("key", key); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Kind defines the kind of certificate to generate from scratch in
// KeyPairAndCA when it's not there.
//
// The two possible kinds are client and server, and they differ in the
// ext-key-usage bitmaps. See GenerateMemCert for more details.
type Kind int

// Possible kinds of certificates.
const (
	CertClient Kind = iota
	CertServer
)

// KeyPairAndCA returns a CertInfo object with a reference to the key pair and
// (optionally) CA certificate located in the given directory and having the
// given name prefix
//
// The naming conversion for the various files is:
//
// <prefix>.crt -> public key
// <prefix>.key -> private key
// <prefix>.ca -> CA certificate
//
// If no public/private key files are found, a new key pair will be generated
// and saved on disk.
//
// If a CA certificate is found, it will be returned as well as second return
// value (otherwise it will be nil).
func KeyPairAndCA(dir, prefix string, kind Kind, options ...Option) (*Info, error) {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return keyPairAndCA(dir, prefix, kind, opts)
}

func keyPairAndCA(dir, prefix string, kind Kind, opts *options) (*Info, error) {
	certFilename := filepath.Join(dir, prefix+".crt")
	keyFilename := filepath.Join(dir, prefix+".key")

	// Ensure that the certificate exists, or create a new one if it does
	// not.
	err := findOrGenCert(certFilename, keyFilename, kind == CertClient, opts)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Load the certificate.
	keypair, err := tls.LoadX509KeyPair(certFilename, keyFilename)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// If available, load the CA data as well.
	caFilename := filepath.Join(dir, prefix+".ca")

	var ca *x509.Certificate
	if opts.fileSystem.Exists(caFilename) {
		if ca, err = readCert(caFilename, opts); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	return &Info{
		keypair: keypair,
		ca:      ca,
	}, nil
}

// ReadCert will read a certificate file and correctly parse it
func ReadCert(path string, options ...Option) (*x509.Certificate, error) {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}
	return readCert(path, opts)
}

func readCert(path string, opts *options) (*x509.Certificate, error) {
	file, err := opts.fileSystem.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer file.Close()

	cf, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	certBlock, _ := pem.Decode(cf)
	if certBlock == nil {
		return nil, errors.New("invalid certificate file")
	}

	return x509.ParseCertificate(certBlock.Bytes)
}

// FindOrGenClientCert will create or generate a certificate
func FindOrGenClientCert(cert, key string, options ...Option) error {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}
	return findOrGenCert(cert, key, true, opts)
}

func findOrGenCert(cert, key string, certType bool, opts *options) error {
	if opts.fileSystem.Exists(cert) && opts.fileSystem.Exists(key) {
		return nil
	}

	generator := NewCertGenerator(
		[]string{"bicycolet.com"},
		WithFileSystem(opts.fileSystem),
		WithClock(opts.clock),
		WithOS(opts.os),
	)

	// If neither stat succeeded, then this is our first run and we need to
	// generate cert and private key
	if err := generator.Generate(cert, key, certType); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
