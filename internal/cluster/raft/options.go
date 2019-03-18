package raft

import (
	"time"

	"github.com/go-kit/kit/log"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/fsys"
)

// Option to be passed to NewRaft to customize the resulting instance.
type Option func(*options)

type options struct {
	database              Node
	mediator              RaftNodeMediator
	dialerProvider        DialerProvider
	addressResolver       AddressResolver
	httpProvider          HTTPProvider
	networkProvider       NetworkProvider
	fileSystem            fsys.FileSystem
	logsProvider          LogsProvider
	snapshotStoreProvider SnapshotStoreProvider
	registryProvider      RegistryProvider
	raftProvider          RaftProvider
	cert                  *cert.Info
	nodeConfigSchema      config.Schema
	latency               float64
	timeout               time.Duration
	logger                log.Logger
}

// WithNode sets the database node on the options
func WithNode(database Node) Option {
	return func(options *options) {
		options.database = database
	}
}

// WithMediator sets the raftNode mediator on the options
func WithMediator(mediator RaftNodeMediator) Option {
	return func(options *options) {
		options.mediator = mediator
	}
}

// WithDialerProvider sets the dialerProvider on the options
func WithDialerProvider(dialerProvider DialerProvider) Option {
	return func(options *options) {
		options.dialerProvider = dialerProvider
	}
}

// WithAddressResolver sets the addressResolver on the options
func WithAddressResolver(addressResolver AddressResolver) Option {
	return func(options *options) {
		options.addressResolver = addressResolver
	}
}

// WithCert sets the certificate information on the options
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

// WithHTTPProvider sets the http provider on the options
func WithHTTPProvider(httpProvider HTTPProvider) Option {
	return func(options *options) {
		options.httpProvider = httpProvider
	}
}

// WithNetworkProvider sets the network provider on the options
func WithNetworkProvider(networkProvider NetworkProvider) Option {
	return func(options *options) {
		options.networkProvider = networkProvider
	}
}

// WithFileSystem sets the file system on the options
func WithFileSystem(fileSystem fsys.FileSystem) Option {
	return func(options *options) {
		options.fileSystem = fileSystem
	}
}

// WithLogsProvider sets the logs provider on the options
func WithLogsProvider(logsProvider LogsProvider) Option {
	return func(options *options) {
		options.logsProvider = logsProvider
	}
}

// WithSnapshotStoreProvider sets the snapshotStore provider on the options
func WithSnapshotStoreProvider(snapshotStoreProvider SnapshotStoreProvider) Option {
	return func(options *options) {
		options.snapshotStoreProvider = snapshotStoreProvider
	}
}

// WithRegistryProvider sets the registry provider on the options
func WithRegistryProvider(registryProvider RegistryProvider) Option {
	return func(options *options) {
		options.registryProvider = registryProvider
	}
}

// WithRaftProvider sets the raft provider on the options
func WithRaftProvider(raftProvider RaftProvider) Option {
	return func(options *options) {
		options.raftProvider = raftProvider
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
		mediator:              raftNodeMediatorShim{},
		dialerProvider:        dialerProviderShim{},
		addressResolver:       addressResolver{},
		httpProvider:          httpProvider{},
		networkProvider:       networkProvider{},
		logsProvider:          logsProvider{},
		snapshotStoreProvider: snapshotStoreProvider{},
		registryProvider:      registryProvider{},
		raftProvider:          raftProvider{},
		latency:               1.0,
		timeout:               5 * time.Second,
		logger:                log.NewNopLogger(),
	}
}
