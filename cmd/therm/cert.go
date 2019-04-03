package main

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/clui"
	"github.com/spoke-d/thermionic/internal/fsys"
)

func hasClientCertificate(fileSystem fsys.FileSystem, path string) bool {
	if !fileSystem.Exists(filepath.Join(path, "client.crt")) {
		return false
	}
	if !fileSystem.Exists(filepath.Join(path, "client.key")) {
		return false
	}
	return true
}

func generateClientCertificate(fileSystem fsys.FileSystem, path string) error {
	if hasClientCertificate(fileSystem, path) {
		return nil
	}

	certPath := filepath.Join(path, "client.crt")
	keyPath := filepath.Join(path, "client.key")
	return cert.FindOrGenClientCert(certPath, keyPath, cert.WithFileSystem(fileSystem))
}

func readCertificates(fileSystem fsys.FileSystem, path string, prompt PasswordPrompt) (certs, error) {
	result := certs{}

	content, err := readFile(fileSystem, filepath.Join(path, "server.crt"))
	if err != nil {
		return certs{}, errors.WithStack(err)
	}
	result.serverCert = content

	result.clientCert, result.clientKey, err = readOrGenerateClientCertificates(fileSystem, path, prompt)
	if err != nil {
		return certs{}, errors.WithStack(err)
	}

	return result, nil
}

func readOrGenerateClientCertificates(fileSystem fsys.FileSystem, path string, prompt PasswordPrompt) (string, string, error) {
	if !hasClientCertificate(fileSystem, path) {
		if err := generateClientCertificate(fileSystem, path); err != nil {
			return "", "", errors.WithStack(err)
		}
	}

	crt, err := readFile(fileSystem, filepath.Join(path, "client.crt"))
	if err != nil {
		return "", "", errors.WithStack(err)
	}

	pemKey, _ := pem.Decode([]byte(crt))
	if x509.IsEncryptedPEMBlock(pemKey) {
		if prompt == nil {
			return "", "", errors.New("No prompt for private key configured")
		}

		password, err := prompt("client.crt")
		if err != nil {
			return "", "", errors.WithStack(err)
		}

		decryptedKey, err := x509.DecryptPEMBlock(pemKey, []byte(password))
		if err != nil {
			return "", "", errors.WithStack(err)
		}
		content := pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: decryptedKey,
		})
		crt = string(content)
	}

	key, err := readFile(fileSystem, filepath.Join(path, "client.key"))
	if err != nil {
		return "", "", errors.WithStack(err)
	}
	return crt, key, errors.WithStack(err)
}

func readFile(fileSystem fsys.FileSystem, path string) (string, error) {
	file, err := fileSystem.Open(path)
	if err != nil {
		return "", errors.WithStack(err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", errors.Wrapf(err, "failed to read %s", path)
	}
	return string(content), nil
}

func askPassword(ui clui.UI) PasswordPrompt {
	return func(query string) (string, error) {
		return ui.AskSecret(fmt.Sprintf("Password for %s: ", query))
	}
}
