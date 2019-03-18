package events

import (
	"github.com/go-kit/kit/log"
	"github.com/spoke-d/thermionic/internal/clock"
)

// Option to be passed to Connect to customize the resulting instance.
type Option func(*options)

type options struct {
	clock clock.Clock
	// Custom logger
	logger log.Logger
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
		logger: log.NewNopLogger(),
		clock:  clock.New(),
	}
}
