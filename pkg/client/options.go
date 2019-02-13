package client

import (
	"net/http"
	"net/url"

	"github.com/go-kit/kit/log"
)

// Option to be passed to Connect to customize the resulting instance.
type Option func(*options)

type options struct {
	// TLS certificate of the remote server. If not specified, the system CA is used.
	tlsServerCert string

	// TLS certificate to use for client authentication.
	tlsClientCert string

	// TLS key to use for client authentication.
	tlsClientKey string

	// TLS CA to validate against when in PKI mode.
	tlsCA string

	// User agent string
	userAgent string

	// Controls whether a client verifies the server's certificate chain and
	// host name.
	insecureSkipVerify bool

	// Custom HTTP Client (used as base for the connection)
	httpClient *http.Client

	// Custom proxy
	proxy func(*http.Request) (*url.URL, error)

	// Custom logger
	logger log.Logger
}

// WithTLSServerCert sets the server cert on the option
func WithTLSServerCert(serverCert string) Option {
	return func(options *options) {
		options.tlsServerCert = serverCert
	}
}

// WithTLSClientCert sets the client cert on the option
func WithTLSClientCert(clientCert string) Option {
	return func(options *options) {
		options.tlsClientCert = clientCert
	}
}

// WithTLSClientKey sets the client key on the option
func WithTLSClientKey(clientKey string) Option {
	return func(options *options) {
		options.tlsClientKey = clientKey
	}
}

// WithTLSCA sets the ca on the option
func WithTLSCA(ca string) Option {
	return func(options *options) {
		options.tlsCA = ca
	}
}

// WithUserAgent sets the userAgent on the option
func WithUserAgent(userAgent string) Option {
	return func(options *options) {
		options.userAgent = userAgent
	}
}

// WithInsecureSkipVerify sets the insecureSkipVerify on the option
func WithInsecureSkipVerify(insecureSkipVerify bool) Option {
	return func(options *options) {
		options.insecureSkipVerify = insecureSkipVerify
	}
}

// WithHTTPClient sets the httpClient on the option
func WithHTTPClient(httpClient *http.Client) Option {
	return func(options *options) {
		options.httpClient = httpClient
	}
}

// WithProxy sets the proxy on the option
func WithProxy(proxy func(*http.Request) (*url.URL, error)) Option {
	return func(options *options) {
		options.proxy = proxy
	}
}

// WithLogger sets the logger on the option
func WithLogger(logger log.Logger) Option {
	return func(options *options) {
		options.logger = logger
	}
}

// Create a options instance with default values.
func newOptions() *options {
	return &options{
		httpClient: http.DefaultClient,
		logger:     log.NewNopLogger(),
	}
}
