package endpoints

import (
	"net/http"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/go-kit/kit/log"
)

// Option to be passed to Connect to customize the resulting instance.
type Option func(*options)

type options struct {
	// HTTP server handling requests for the RESTful API.
	restServer *http.Server

	// The TLS keypair and optional CA to use for the network endpoint. It
	// must be always provided, since the public key will be included in
	// the response of the /1.0 REST API as part of the server info.
	//
	// It can be updated after the endpoints are up using NetworkUpdateCert().
	cert *cert.Info

	// NetworkSetAddress sets the address for the network endpoint. If not
	// set, the network endpoint won't be started (unless it's passed via
	// socket-based activation).
	//
	// It can be updated after the endpoints are up using UpdateNetworkAddress().
	networkAddress string

	// DebugSetAddress sets the address for the pprof endpoint.
	//
	// It can be updated after the endpoints are up using UpdateDebugAddress().
	debugAddress string

	// Custom logger
	logger  log.Logger
	sleeper clock.Sleeper
}

// WithRestServer sets the restServer on the option
func WithRestServer(restServer *http.Server) Option {
	return func(options *options) {
		options.restServer = restServer
	}
}

// WithCert sets the cert on the option
func WithCert(cert *cert.Info) Option {
	return func(options *options) {
		options.cert = cert
	}
}

// WithNetworkAddress sets the networkAddress on the option
func WithNetworkAddress(networkAddress string) Option {
	return func(options *options) {
		options.networkAddress = networkAddress
	}
}

// WithDebugAddress sets the debugAddress on the option
func WithDebugAddress(debugAddress string) Option {
	return func(options *options) {
		options.debugAddress = debugAddress
	}
}

// WithLogger sets the logger on the option
func WithLogger(logger log.Logger) Option {
	return func(options *options) {
		options.logger = logger
	}
}

// WithSleeper sets the sleeper on the options
func WithSleeper(sleeper clock.Sleeper) Option {
	return func(options *options) {
		options.sleeper = sleeper
	}
}

// Create a options instance with default values.
func newOptions() *options {
	return &options{
		logger:  log.NewNopLogger(),
		sleeper: clock.DefaultSleeper,
	}
}
