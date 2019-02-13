package operations

import "github.com/go-kit/kit/log"

// Option to be passed to Connect to customize the resulting instance.
type Option func(*options)

type options struct {
	logger log.Logger
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
		logger: log.NewNopLogger(),
	}
}
