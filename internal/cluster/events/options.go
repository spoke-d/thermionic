package events

import (
	"github.com/go-kit/kit/log"
	"github.com/spoke-d/thermionic/internal/clock"
)

// Option to be passed to Connect to customize the resulting instance.
type Option func(*options)

type options struct {
	eventsSourceProvider EventsSourceProvider
	clock                clock.Clock
	logger               log.Logger
}

// WithEventsSourceProvider sets the eventsSourceProvider on the option
func WithEventsSourceProvider(eventsSourceProvider EventsSourceProvider) Option {
	return func(options *options) {
		options.eventsSourceProvider = eventsSourceProvider
	}
}

// WithClock sets the clock on the option
func WithClock(clock clock.Clock) Option {
	return func(options *options) {
		options.clock = clock
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
		eventsSourceProvider: eventsSourceProvider{},
		clock:                clock.New(),
		logger:               log.NewNopLogger(),
	}
}
