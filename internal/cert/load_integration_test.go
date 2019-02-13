// +build integration

package cert_test

import (
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/fsys"
)

// A new key pair is generated if none exists and saved to the appropriate
// files.
func TestKeyPairAndCA(t *testing.T) {
	fileSystem := fsys.NewLocalFileSystem(false)

	dir, err := ioutil.TempDir("", "cert-test-")
	if err != nil {
		t.Fatalf("failed to create temporary dir: %v", err)
	}
	defer os.RemoveAll(dir)

	info, err := cert.KeyPairAndCA(dir, "test", cert.CertServer,
		cert.WithFileSystem(fileSystem),
	)
	if err != nil {
		t.Fatalf("initial call to KeyPairAndCA failed: %+v", err)
	}
	if info.CA() != nil {
		t.Errorf("expected CA certificate to be nil")
	}
	if len(info.KeyPair().Certificate) == 0 {
		t.Errorf("expected key pair to be non-empty")
	}
	if !fileSystem.Exists(filepath.Join(dir, "test.crt")) {
		t.Errorf("no public key file was saved")
	}
	if !fileSystem.Exists(filepath.Join(dir, "test.key")) {
		t.Errorf("no secret key file was saved")
	}

	cert, err := x509.ParseCertificate(info.KeyPair().Certificate[0])
	if err != nil {
		t.Errorf("failed to parse generated public x509 key cert: %v", err)
	}
	if cert.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
		t.Errorf("expected to find server auth key usage extension")
	}

	block, _ := pem.Decode(info.PublicKey())
	if block == nil {
		t.Errorf("expected PublicKey to be decodable")
	}
	_, err = x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Errorf("failed to parse encoded public x509 key cert: %v", err)
	}
}
