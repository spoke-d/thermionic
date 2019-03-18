package cert

import (
	"github.com/go-kit/kit/log"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/sys"
)

// Option to be passed to Connect to customize the resulting instance.
type Option func(*options)

type options struct {
	fileSystem fsys.FileSystem
	os         OS
	clock      clock.Clock
	logger     log.Logger
}

// WithFileSystem sets the fileSystem on the option
func WithFileSystem(fileSystem fsys.FileSystem) Option {
	return func(options *options) {
		options.fileSystem = fileSystem
	}
}

// WithOS sets the os on the option
func WithOS(os OS) Option {
	return func(options *options) {
		options.os = os
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
		fileSystem: fsys.NewVirtualFileSystem(),
		os:         sys.DefaultOS(),
		clock:      clock.New(),
		logger:     log.NewNopLogger(),
	}
}
