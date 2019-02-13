package fsys

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// FileSystem is an abstraction over the native filesystem
type FileSystem interface {
	// Create takes a path, creates the file and then returns a File back that
	// can be used. This returns an error if the file can not be created in
	// some way.
	Create(string) (File, error)

	// Open takes a path, opens a potential file and then returns a File if
	// that file exists, otherwise it returns an error if the file wasn't found.
	Open(string) (File, error)

	// OpenFile takes a path, opens a potential file and then returns a File if
	// that file exists, otherwise it returns an error if the file wasn't found.
	OpenFile(path string, flag int, perm os.FileMode) (File, error)

	// Rename takes a current destination path and a new destination path and will
	// rename the a File if it exists, otherwise it returns an error if the file
	// wasn't found.
	Rename(string, string) error

	// Exists takes a path and checks to see if the potential file exists or
	// not.
	// Note: If there is an error trying to read that file, it will return false
	// even if the file already exists.
	Exists(string) bool

	// Remove takes a path, removes a potential file, if no file doesn't exist it
	// will return not found.
	Remove(string) error

	// RemoveAll takes a path, removes all potential files and directories, if
	// no file or directory doesn't exist it will return not found.
	RemoveAll(string) error

	// Mkdir takes a path and generates a directory structure from that path,
	// with the given file mode and if there is a failure it will return an error.
	Mkdir(string, os.FileMode) error

	// MkdirAll takes a path and generates a directory structure from that path,
	// if there is a failure it will return an error.
	MkdirAll(string, os.FileMode) error

	// Chtimes updates the modifier times for a given path or returns an error
	// upon failure
	Chtimes(string, time.Time, time.Time) error

	// Walk over a specific directory and will return an error if there was an
	// error whilst walking.
	Walk(string, filepath.WalkFunc) error

	// Lock attempts to create a locking file for a given path.
	Lock(string) (Releaser, bool, error)

	// CopyFile copies a directory recursively, overwriting the target if it
	// exists.
	CopyFile(string, string) error

	// CopyDir copies a directory recursively, overwriting the target if it
	// exists.
	CopyDir(string, string) error

	// Symlink creates newname as a symbolic link to oldname.
	// If there is an error, it will be of type *LinkError.
	Symlink(string, string) error
}

// File is an abstraction for reading, writing and also closing a file. These
// interfaces already exist, it's just a matter of composing them to be more
// usable by other components.
type File interface {
	io.Reader
	io.Writer
	io.Closer

	// Name returns the name of the file
	Name() string

	// Size returns the size of the file
	Size() int64

	// Sync attempts to sync the file with the underlying storage or errors if it
	// can't not succeed.
	Sync() error
}

// Releaser is returned by Lock calls.
type Releaser interface {

	// Release given lock or returns error upon failure.
	Release() error
}

type notFound interface {
	NotFound() bool
}

type errNotFound struct {
	err error
}

// NotFound creates a new not found error
func NotFound(err error) error {
	return errNotFound{err}
}

func (e errNotFound) Error() string {
	return e.err.Error()
}

func (e errNotFound) NotFound() bool {
	return true
}

// ErrNotFound tests to see if the error passed is a not found error or not.
func ErrNotFound(err error) bool {
	if err != nil {
		if _, ok := err.(notFound); ok {
			return true
		}
	}
	return false
}

// Config encapsulates the requirements for generating a FileSystem
type Config struct {
	name string
	mmap bool
}

// Option defines a option for generating a filesystem Config
type Option func(*Config) error

// Build ingests configuration options to then yield a Config and return an
// error if it fails during setup.
func Build(opts ...Option) (*Config, error) {
	var config Config
	for _, opt := range opts {
		err := opt(&config)
		if err != nil {
			return nil, err
		}
	}
	return &config, nil
}

// With adds a type of filesystem to use for the configuration.
func With(name string) Option {
	return func(config *Config) error {
		config.name = name
		return nil
	}
}

// WithMMAP defines if we should use mmap or not.
func WithMMAP(mmap bool) Option {
	return func(config *Config) error {
		config.mmap = mmap
		return nil
	}
}

// New creates a filesystem from a configuration or returns error if on failure.
func New(config *Config) (fsys FileSystem, err error) {
	switch strings.ToLower(config.name) {
	case "local":
		fsys = NewLocalFileSystem(config.mmap)
	case "virtual":
		fsys = NewVirtualFileSystem()
	default:
		err = errors.Errorf("unexpected fs type %q", config.name)
	}
	return
}
