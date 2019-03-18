package api

import (
	"time"

	"github.com/go-kit/kit/log"
	"github.com/spoke-d/thermionic/internal/clock"
)

// Option to be passed to Connect to customize the resulting instance.
type Option func(*options)

type options struct {
	services                 []Service
	internalServices         []Service
	clock                    clock.Clock
	certificateCacheDuration time.Duration
	// Custom logger
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
		logger:                   log.NewNopLogger(),
		clock:                    clock.New(),
		certificateCacheDuration: time.Hour * 24,
	}
}
