package db

import (
	"github.com/go-kit/kit/log"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/fsys"
)

// ClusterOption to be passed to NewCluster to customize the resulting instance.
type ClusterOption func(*clusterOptions)

type clusterOptions struct {
	clusterTxProvider ClusterTxProvider
	fileSystem        fsys.FileSystem
	logger            log.Logger
	transaction       Transaction
	clock             clock.Clock
	sleeper           clock.Sleeper
}

// WithClusterTxProviderForCluster sets the clusterTxProvider on the clusterOptions
func WithClusterTxProviderForCluster(clusterTxProvider ClusterTxProvider) ClusterOption {
	return func(clusterOptions *clusterOptions) {
		clusterOptions.clusterTxProvider = clusterTxProvider
	}
}

// WithFileSystemForCluster sets the fileSystem on the clusterOptions
func WithFileSystemForCluster(fileSystem fsys.FileSystem) ClusterOption {
	return func(clusterOptions *clusterOptions) {
		clusterOptions.fileSystem = fileSystem
	}
}

// WithTransactionForCluster sets the transaction on the clusterOptions
func WithTransactionForCluster(transaction Transaction) ClusterOption {
	return func(clusterOptions *clusterOptions) {
		clusterOptions.transaction = transaction
	}
}

// WithLoggerForCluster sets the logger on the clusterOptions
func WithLoggerForCluster(logger log.Logger) ClusterOption {
	return func(clusterOptions *clusterOptions) {
		clusterOptions.logger = logger
	}
}

// WithClockForCluster sets the clock on the clusterOptions
func WithClockForCluster(clock clock.Clock) ClusterOption {
	return func(clusterOptions *clusterOptions) {
		clusterOptions.clock = clock
	}
}

// WithSleeperForCluster sets the sleeper on the clusterOptions
func WithSleeperForCluster(sleeper clock.Sleeper) ClusterOption {
	return func(clusterOptions *clusterOptions) {
		clusterOptions.sleeper = sleeper
	}
}

// Create a clusterOptions instance with default values.
func newClusterOptions() *clusterOptions {
	return &clusterOptions{
		clusterTxProvider: clusterTxProvider{},
		transaction:       transactionShim{},
		clock:             clock.New(),
		sleeper:           clock.DefaultSleeper,
		logger:            log.NewNopLogger(),
	}
}

type clusterTxProvider struct{}

func (clusterTxProvider) New(tx database.Tx, nodeID int64) *ClusterTx {
	return NewClusterTx(tx, nodeID)
}
