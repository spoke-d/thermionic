package members

import (
	"github.com/go-kit/kit/log"
)

// options defines a configuration setup for creating a list to manage the
// members cluster
type options struct {
	existing []string
	logger   log.Logger
}

// Option defines a option for generating a filesystem options
type Option func(*options)

// WithExisting adds a Existing to the configuration
func WithExisting(existing []string) Option {
	return func(options *options) {
		options.existing = existing
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
		logger: log.NewNopLogger(),
	}
}
