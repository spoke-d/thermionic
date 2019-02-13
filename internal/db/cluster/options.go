package cluster

import (
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/fsys"
)

// Option to be passed to NewRaft to customize the resulting instance.
type Option func(*options)

type options struct {
	database       database.DB
	databaseIO     DatabaseIO
	fileSystem     fsys.FileSystem
	nameProvider   NameProvider
	apiExtensions  APIExtensions
	schemaProvider SchemaProvider
	sleeper        clock.Sleeper
}

// WithDatabase sets the database on the options
func WithDatabase(database database.DB) Option {
	return func(options *options) {
		options.database = database
	}
}

// WithDatabaseIO sets the database IO on the options
func WithDatabaseIO(databaseIO DatabaseIO) Option {
	return func(options *options) {
		options.databaseIO = databaseIO
	}
}

// WithFileSystem sets the file system on the options
func WithFileSystem(fileSystem fsys.FileSystem) Option {
	return func(options *options) {
		if fileSystem != nil {
			options.fileSystem = fileSystem
		}
	}
}

// WithNameProvider sets the name provider on the options
func WithNameProvider(nameProvider NameProvider) Option {
	return func(options *options) {
		// NameProvider is optional from the cluster, so we only ever want to
		// override this when it's valid.
		if nameProvider != nil {
			options.nameProvider = nameProvider
		}
	}
}

// WithAPIExtensions sets the api extensions on the options
func WithAPIExtensions(apiExtensions APIExtensions) Option {
	return func(options *options) {
		if apiExtensions != nil {
			options.apiExtensions = apiExtensions
		}
	}
}

// WithSchemaProvider sets the schema provider on the options
func WithSchemaProvider(schemaProvider SchemaProvider) Option {
	return func(options *options) {
		options.schemaProvider = schemaProvider
	}
}

// WithSleeper sets the sleeper on the options
func WithSleeper(sleeper clock.Sleeper) Option {
	return func(options *options) {
		if sleeper != nil {
			options.sleeper = sleeper
		}
	}
}

// Create a options instance with default values.
func newOptions() *options {
	return &options{
		databaseIO:   databaseIO{},
		nameProvider: &DQliteNameProvider{},
		sleeper:      clock.DefaultSleeper,
	}
}
