package cert

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// InitTLSConfig returns a tls.Config populated with default encryption
// parameters. This is used as baseline config for both client and server
// certificates used by thermionic.
func InitTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		MaxVersion: tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		},
		PreferServerCipherSuites: true,
	}
}

// TLSClientConfig returns a TLS configuration suitable for establishing
// inter-node network connections using the cluster certificate.
func TLSClientConfig(info *Info) (*tls.Config, error) {
	keypair := info.KeyPair()
	ca := info.CA()

	config := InitTLSConfig()
	config.Certificates = []tls.Certificate{
		keypair,
	}
	config.RootCAs = x509.NewCertPool()
	if ca != nil {
		config.RootCAs.AddCert(ca)
	}

	// Since the same cluster keypair is used both as server and as client
	// cert, let's add it to the CA pool to make it trusted.
	cert, err := x509.ParseCertificate(keypair.Certificate[0])
	if err != nil {
		return nil, errors.WithStack(err)
	}
	cert.IsCA = true
	cert.KeyUsage = x509.KeyUsageCertSign
	config.RootCAs.AddCert(cert)

	if cert.DNSNames != nil {
		config.ServerName = cert.DNSNames[0]
	}
	return config, nil
}

// TLSCheckCert returns true if the given request is presenting the given
// cluster certificate.
func TLSCheckCert(r *http.Request, info *Info) (bool, error) {
	cert, err := x509.ParseCertificate(info.KeyPair().Certificate[0])
	if err != nil {
		// Since we have already loaded this certificate, typically
		// using LoadX509KeyPair, an error should never happen, but
		// check for good measure.
		return false, errors.Wrap(err, "invalid keypair material")
	}
	trustedCerts := []x509.Certificate{
		*cert,
	}
	return r.TLS != nil && CheckTrustState(*r.TLS.PeerCertificates[0], trustedCerts), nil
}

// CheckTrustState checks whether the given client certificate is trusted
// (i.e. it has a valid time span and it belongs to the given list of trusted
// certificates).
func CheckTrustState(cert x509.Certificate, trustedCerts []x509.Certificate) bool {
	// Extra validity check (should have been caught by TLS stack)
	if time.Now().Before(cert.NotBefore) || time.Now().After(cert.NotAfter) {
		return false
	}

	for _, v := range trustedCerts {
		if bytes.Compare(cert.Raw, v.Raw) == 0 {
			return true
		}
	}

	return false
}

// ServerTLSConfig returns a new server-side tls.Config generated from the give
// certificate info.
func ServerTLSConfig(cert *Info, logger log.Logger) *tls.Config {
	config := InitTLSConfig()
	config.ClientAuth = tls.RequestClientCert
	config.Certificates = []tls.Certificate{cert.KeyPair()}
	config.NextProtos = []string{"h2"} // Required by gRPC

	if cert.CA() != nil {
		pool := x509.NewCertPool()
		pool.AddCert(cert.CA())
		config.RootCAs = pool
		config.ClientCAs = pool

		level.Info(logger).Log("msg", "CA mode, only CA-signed certificates will be allowed")
	}

	config.BuildNameToCertificate()
	return config
}

func GetTLSConfigMem(
	tlsClientCert,
	tlsClientKey,
	tlsClientCA,
	tlsRemoteCertPEM string,
	insecureSkipVerify bool,
) (*tls.Config, error) {
	tlsConfig := InitTLSConfig()
	tlsConfig.InsecureSkipVerify = insecureSkipVerify
	// Client authentication
	if tlsClientCert != "" && tlsClientKey != "" {
		cert, err := tls.X509KeyPair([]byte(tlsClientCert), []byte(tlsClientKey))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		tlsConfig.Certificates = []tls.Certificate{
			cert,
		}
	}

	var tlsRemoteCert *x509.Certificate
	if tlsRemoteCertPEM != "" {
		// Ignore any content outside of the PEM bytes we care about
		certBlock, _ := pem.Decode([]byte(tlsRemoteCertPEM))
		if certBlock == nil {
			return nil, errors.Errorf("Invalid remote certificate")
		}

		var err error
		tlsRemoteCert, err = x509.ParseCertificate(certBlock.Bytes)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	if tlsClientCA != "" {
		caPool := x509.NewCertPool()
		caPool.AppendCertsFromPEM([]byte(tlsClientCA))

		tlsConfig.RootCAs = caPool
	}

	finalizeTLSConfig(tlsConfig, tlsRemoteCert)

	return tlsConfig, nil
}

func finalizeTLSConfig(tlsConfig *tls.Config, tlsRemoteCert *x509.Certificate) {
	// Trusted certificates
	if tlsRemoteCert != nil {
		caCertPool := tlsConfig.RootCAs
		if caCertPool == nil {
			var err error
			caCertPool, err = x509.SystemCertPool()
			if err != nil || caCertPool == nil {
				caCertPool = x509.NewCertPool()
			}
		}

		// Make it a valid RootCA
		tlsRemoteCert.IsCA = true
		tlsRemoteCert.KeyUsage = x509.KeyUsageCertSign

		// Setup the pool
		caCertPool.AddCert(tlsRemoteCert)
		tlsConfig.RootCAs = caCertPool

		// Set the ServerName
		if tlsRemoteCert.DNSNames != nil {
			tlsConfig.ServerName = tlsRemoteCert.DNSNames[0]
		}
	}

	tlsConfig.BuildNameToCertificate()
}
