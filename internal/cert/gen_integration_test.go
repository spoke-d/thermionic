// +build integration

package cert_test

import (
	"encoding/pem"
	"testing"

	"github.com/spoke-d/thermionic/internal/cert"
)

func TestGenerateMemCert(t *testing.T) {
	generate := cert.NewCertGenerator([]string{""})

	certKey, err := generate.GenerateMemCert(false)
	if err != nil {
		t.Fatal(err)
	}
	if certKey.Cert == nil {
		t.Fatal("GenerateMemCert returned a nil cert")
	}
	if certKey.Key == nil {
		t.Fatal("GenerateMemCert returned a nil key")
	}

	block, rest := pem.Decode(certKey.Cert)
	if len(rest) != 0 {
		t.Errorf("GenerateMemCert returned a cert with trailing content: %q", string(rest))
	}
	if block.Type != "CERTIFICATE" {
		t.Errorf("GenerateMemCert returned a cert with Type %q not \"CERTIFICATE\"", block.Type)
	}
	block, rest = pem.Decode(certKey.Key)
	if len(rest) != 0 {
		t.Errorf("GenerateMemCert returned a key with trailing content: %q", string(rest))
	}
	if block.Type != "RSA PRIVATE KEY" {
		t.Errorf("GenerateMemCert returned a cert with Type %q not \"RSA PRIVATE KEY\"", block.Type)
	}
}
