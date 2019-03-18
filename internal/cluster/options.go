package cluster

import (
	"github.com/go-kit/kit/log"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/fsys"
)

// Option to be passed to NewGateway to customize the resulting instance.
type Option func(*options)

type options struct {
	database         Node
	cert             *cert.Info
	nodeConfigSchema config.Schema
	fileSystem       fsys.FileSystem
	raftProvider     RaftProvider
	netProvider      Net
	storeProvider    StoreProvider
	serverProvider   ServerProvider
	sleeper          clock.Sleeper
	logger           log.Logger
	latency          float64
}

// WithNode sets the database node on the options
func WithNode(database Node) Option {
	return func(options *options) {
		options.database = database
	}
}

// WithRaftProvider sets the raftProvider on the options
func WithRaftProvider(raftProvider RaftProvider) Option {
	return func(options *options) {
		options.raftProvider = raftProvider
	}
}

// WithNet sets the netProvider on the options
func WithNet(netProvider Net) Option {
	return func(options *options) {
		options.netProvider = netProvider
	}
}

// WithStoreProvider sets the storeProvider on the options
func WithStoreProvider(storeProvider StoreProvider) Option {
	return func(options *options) {
		options.storeProvider = storeProvider
	}
}

// WithServerProvider sets the serverProvider on the options
func WithServerProvider(serverProvider ServerProvider) Option {
	return func(options *options) {
		options.serverProvider = serverProvider
	}
}

// WithSleeper sets the sleeper on the options
func WithSleeper(sleeper clock.Sleeper) Option {
	return func(options *options) {
		options.sleeper = sleeper
	}
}

// WithCert sets the cert on the options
func WithCert(cert *cert.Info) Option {
	return func(options *options) {
		options.cert = cert
	}
}

// WithNodeConfigSchema sets the node configuration schema on the options
func WithNodeConfigSchema(nodeConfigSchema config.Schema) Option {
	return func(options *options) {
		options.nodeConfigSchema = nodeConfigSchema
	}
}

// WithFileSystem sets the fileSystem on the options
func WithFileSystem(fileSystem fsys.FileSystem) Option {
	return func(options *options) {
		options.fileSystem = fileSystem
	}
}

// WithLogger sets the logger on the options
func WithLogger(logger log.Logger) Option {
	return func(options *options) {
		options.logger = logger
	}
}

// WithLatency is a coarse grain measure of how fast/reliable network links
// are. This is used to tweak the various timeouts parameters of the raft
// algorithm. See the raft.Config structure for more details. A value of 1.0
// means use the default values from hashicorp's raft package. Values closer to
// 0 reduce the values of the various timeouts (useful when running unit tests
// in-memory).
func WithLatency(latency float64) Option {
	return func(options *options) {
		options.latency = latency
	}
}

// Create a options instance with default values.
func newOptions() *options {
	return &options{
		raftProvider:   raftProvider{},
		netProvider:    netProvider{},
		storeProvider:  storeProvider{},
		serverProvider: serverProvider{},
		sleeper:        clock.DefaultSleeper,
		logger:         log.NewNopLogger(),
		latency:        1.0,
	}
}
