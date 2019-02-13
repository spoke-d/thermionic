package discovery

import (
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/go-kit/kit/log"
)

// Option to be passed to Connect to customize the resulting instance.
type Option func(*options)

type options struct {
	os         OS
	fileSystem fsys.FileSystem
	logger     log.Logger
	sleeper    clock.Sleeper
}

// WithOS sets the os on the options
func WithOS(os OS) Option {
	return func(options *options) {
		options.os = os
	}
}

// WithFileSystem sets the fileSystem on the options
func WithFileSystem(fileSystem fsys.FileSystem) Option {
	return func(options *options) {
		options.fileSystem = fileSystem
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
		logger:  log.NewNopLogger(),
		sleeper: clock.DefaultSleeper,
	}
}
