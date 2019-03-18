package services

import (
	"github.com/go-kit/kit/log"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/fsys"
)

// Option to be passed to Connect to customize the resulting instance.
type Option func(*options)

type options struct {
	logger     log.Logger
	fileSystem fsys.FileSystem
	clock      clock.Clock
}

// WithLogger sets the logger on the option
func WithLogger(logger log.Logger) Option {
	return func(options *options) {
		options.logger = logger
	}
}

// WithFileSystem sets the fileSystem on the option
func WithFileSystem(fileSystem fsys.FileSystem) Option {
	return func(options *options) {
		options.fileSystem = fileSystem
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
		logger:     log.NewNopLogger(),
		fileSystem: fsys.NewLocalFileSystem(false),
		clock:      clock.New(),
	}
}
