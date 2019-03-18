package db

import (
	"fmt"
	"sync"
	"time"

	"github.com/CanonicalLtd/go-dqlite"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/db/database"
	q "github.com/spoke-d/thermionic/internal/db/query"
)

// ClusterTransactioner represents a way to run transaction on the cluster
type ClusterTransactioner interface {

	// Transaction creates a new ClusterTx object and transactionally executes the
	// cluster database interactions invoked by the given function. If the function
	// returns no error, all database changes are committed to the cluster database
	// database, otherwise they are rolled back.
	//
	// If EnterExclusive has been called before, calling Transaction will block
	// until ExitExclusive has been called as well to release the lock.
	Transaction(f func(*ClusterTx) error) error
}

// ClusterOpener represents a way to open a cluster
type ClusterOpener interface {

	// Open creates a new Cluster object for interacting with the dqlite database.
	//
	// - store: Function used to connect to the dqlite backend.
	// - address: Network address of this node (or empty string).
	// - dir: Base database directory (e.g. /var/lib/thermionic/database)
	// - options: Driver options that can be passed to the cluster when opening the db.
	//
	// The address and api parameters will be used to determine if the cluster
	// database matches our version, and possibly trigger a schema update. If the
	// schema update can't be performed right now, because some nodes are still
	// behind, an Upgrading error is returned.
	Open(cluster.ServerStore, string, string, time.Duration, ...dqlite.DriverOption) error
}

// ClusterExclusiveLocker defines an exclusive lock for doing certain operations
// on the cluster.
type ClusterExclusiveLocker interface {

	// EnterExclusive acquires a lock on the cluster db, so any successive call to
	// Transaction will block until ExitExclusive has been called.
	EnterExclusive() error

	// ExitExclusive runs the given transaction and then releases the lock acquired
	// with EnterExclusive.
	ExitExclusive(func(*ClusterTx) error) error
}

// ErrSomeNodesAreBehind is returned by OpenCluster if some of the nodes in the
// cluster have a schema or API version that is less recent than this node.
var ErrSomeNodesAreBehind = errors.Errorf("some nodes are behind this node's version")

// QueryCluster defines an interface for interacting with the cluster DB
type QueryCluster interface {
	database.DBAccessor

	// Open the cluster database object.
	//
	// The name argument is the name of the cluster database. It defaults to
	// 'db.bin', but can be overwritten for testing.
	//
	// The dialer argument is a function that returns a gRPC dialer that can be
	// used to connect to a database node using the gRPC SQL package.
	Open(cluster.ServerStore, ...dqlite.DriverOption) error

	// EnsureSchema applies all relevant schema updates to the cluster database.
	//
	// Before actually doing anything, this function will make sure that all nodes
	// in the cluster have a schema version and a number of API extensions that
	// match our one. If it's not the case, we either return an error (if some
	// nodes have version greater than us and we need to be upgraded), or return
	// false and no error (if some nodes have a lower version, and we need to wait
	// till they get upgraded and restarted).
	EnsureSchema(string, string) (bool, error)

	// SchemaVersion returns the underlying schema version for the cluster
	SchemaVersion() int
}

// ClusterTxProvider creates ClusterTx which can be used by the cluster
type ClusterTxProvider interface {

	// New creates a ClusterTx with the sane defaults
	New(database.Tx, int64) *ClusterTx
}

// Cluster mediates access to data stored in the cluster dqlite database.
type Cluster struct {
	cluster           QueryCluster // Handle to the cluster dqlite database, gated behind gRPC SQL.
	clusterTxProvider ClusterTxProvider
	nodeID            int64 // Node ID of this instance.
	mu                sync.RWMutex
	transaction       Transaction // Handle the transactions to the database
	logger            log.Logger
	clock             clock.Clock
	sleeper           clock.Sleeper
}

// NewCluster creates a new Cluster object.
func NewCluster(cluster QueryCluster, options ...ClusterOption) *Cluster {
	opts := newClusterOptions()
	for _, option := range options {
		option(opts)
	}

	return &Cluster{
		cluster:           cluster,
		clusterTxProvider: opts.clusterTxProvider,
		transaction:       opts.transaction,
		logger:            opts.logger,
		clock:             opts.clock,
		sleeper:           opts.sleeper,
	}
}

// Open creates a new Cluster object for interacting with the dqlite database.
//
// - store: Function used to connect to the dqlite backend.
// - address: Network address of this node (or empty string).
// - dir: Base database directory (e.g. /var/lib/thermionic/database)
// - options: Driver options that can be passed to the cluster when opening the db.
//
// The address and api parameters will be used to determine if the cluster
// database matches our version, and possibly trigger a schema update. If the
// schema update can't be performed right now, because some nodes are still
// behind, an Upgrading error is returned.
func (c *Cluster) Open(
	store cluster.ServerStore,
	address, dir string,
	timeout time.Duration,
	options ...dqlite.DriverOption,
) error {
	if err := c.cluster.Open(store, options...); err != nil {
		return errors.Wrap(err, "failed to open database")
	}

	// Test that the cluster database is operational. We wait up to 10
	// minutes, in case there's no quorum of nodes online yet.
	after := c.clock.After(timeout)
	for i := 0; ; i++ {
		// Log initial attempts at debug level, but use warn
		// level after the 5'th attempt (about 10 seconds).
		// After the 15'th attempt (about 30 seconds), log
		// only one attempt every 5.
		logPriority := 1 // 0 is discard, 1 is Debug, 2 is Warn
		if i > 5 {
			logPriority = 2
			if i > 15 && !((i % 5) == 0) {
				logPriority = 0
			}
		}

		err := c.cluster.DB().Ping()
		if err == nil {
			break
		}
		if errors.Cause(err) != dqlite.ErrNoAvailableLeader {
			return errors.WithStack(err)
		}

		msg := fmt.Sprintf("Failed connecting to global database (attempt %d)", i)
		switch logPriority {
		case 1:
			level.Debug(c.logger).Log("err", err, "msg", msg)
		case 2:
			level.Warn(c.logger).Log("err", err, "msg", msg)
		}

		c.sleeper.Sleep(2 * time.Second)
		select {
		case <-after:
			return errors.Errorf("failed to connect to cluster database")
		default:
		}
	}

	nodesVersionsMatch, err := c.cluster.EnsureSchema(address, dir)
	if err != nil {
		return errors.Wrap(err, "failed to ensure schema")
	}

	// Figure out the ID of this node.
	if err := c.Transaction(func(tx *ClusterTx) error {
		nodes, err := tx.Nodes()
		if err != nil {
			return errors.Wrap(err, "failed to fetch nodes")
		}
		if len(nodes) == 1 && nodes[0].Address == "0.0.0.0" {
			// We're not clustered
			c.NodeID(1)
			return nil
		}
		for _, node := range nodes {
			if node.Address == address {
				c.nodeID = node.ID
				return nil
			}
		}
		return errors.Errorf("no node registered with address %q", address)
	}); err != nil {
		return errors.WithStack(err)
	}

	if !nodesVersionsMatch {
		err = ErrSomeNodesAreBehind
	}
	return err
}

// Transaction creates a new ClusterTx object and transactionally executes the
// cluster database interactions invoked by the given function. If the function
// returns no error, all database changes are committed to the cluster database
// database, otherwise they are rolled back.
//
// If EnterExclusive has been called before, calling Transaction will block
// until ExitExclusive has been called as well to release the lock.
func (c *Cluster) Transaction(f func(*ClusterTx) error) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tx(f)
}

// NodeID sets the the node NodeID associated with this cluster instance. It's used for
// backward-compatibility of all db-related APIs that were written before
// clustering and don't accept a node NodeID, so in those cases we automatically
// use this value as implicit node NodeID.
func (c *Cluster) NodeID(id int64) {
	c.nodeID = id
}

// DB returns the underlying database
func (c *Cluster) DB() database.DB {
	return c.cluster.DB()
}

// SchemaVersion returns the underlying schema version for the cluster
func (c *Cluster) SchemaVersion() int {
	return c.cluster.SchemaVersion()
}

// Close the database facade.
func (c *Cluster) Close() error {
	return c.cluster.DB().Close()
}

// EnterExclusive acquires a lock on the cluster db, so any successive call to
// Transaction will block until ExitExclusive has been called.
func (c *Cluster) EnterExclusive() error {
	level.Debug(c.logger).Log("msg", "Acquiring exclusive lock on cluster db")

	ch := make(chan struct{})
	go func() {
		c.mu.Lock()
		ch <- struct{}{}
	}()

	timeout := 20 * time.Second
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return errors.Errorf("timeout (%s)", timeout)
	}
}

// ExitExclusive runs the given transaction and then releases the lock acquired
// with EnterExclusive.
func (c *Cluster) ExitExclusive(f func(*ClusterTx) error) error {
	level.Debug(c.logger).Log("msg", "Releasing exclusive lock on cluster db")
	defer c.mu.Unlock()
	return c.tx(f)
}

func (c *Cluster) tx(f func(*ClusterTx) error) error {
	nodeID := c.nodeID
	return q.Retry(c.sleeper, func() error {
		return c.transaction.Transaction(c.cluster.DB(), func(tx database.Tx) error {
			return f(c.clusterTxProvider.New(tx, nodeID))
		})
	})
}

// UnsafeClusterDB accesses the cluster database, mainly for tests, so that we
// don't expose the DB directly on the Cluster.
func UnsafeClusterDB(c *Cluster) database.DB {
	return c.cluster.DB()
}

// UnsafeNodeID accesses the node ID from the cluster, mainly for tests, so
// that we don't expose the node ID directly on the Cluster.
func UnsafeNodeID(c *Cluster) int64 {
	return c.nodeID
}
