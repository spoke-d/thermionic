package notifier

import (
	"net/http"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/go-kit/kit/log"
)

// Option to be passed to Connect to customize the resulting instance.
type Option func(*options)

type options struct {
	state      State
	certInfo   *cert.Info
	httpClient *http.Client
	logger     log.Logger
	clock      clock.Clock
}

// WithState sets the state on the options
func WithState(state State) Option {
	return func(options *options) {
		options.state = state
	}
}

// WithCert sets the certInfo on the options
func WithCert(certInfo *cert.Info) Option {
	return func(options *options) {
		options.certInfo = certInfo
	}
}

// WithHTTPClient sets the httpClient on the option
func WithHTTPClient(httpClient *http.Client) Option {
	return func(options *options) {
		options.httpClient = httpClient
	}
}

// WithLogger sets the logger on the option
func WithLogger(logger log.Logger) Option {
	return func(options *options) {
		options.logger = logger
	}
}

// WithClock sets the clock on the option
func WithClock(clock clock.Clock) Option {
	return func(options *options) {
		options.clock = clock
	}
}

// Create a options instance with default values.
func newOptions() *options {
	return &options{
		httpClient: http.DefaultClient,
		logger:     log.NewNopLogger(),
		clock:      clock.New(),
	}
}
