package membership

import (
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/fsys"
)

// JoinOption to be passed to NewJoin to customize the resulting
// instance.
type JoinOption func(*joinOptions)

type joinOptions struct {
	state            State
	gateway          Gateway
	nodeConfigSchema config.Schema
	config           NodeConfigProvider
	fileSystem       fsys.FileSystem
	logger           log.Logger
}

// WithStateForJoin sets the state on the options
func WithStateForJoin(state State) JoinOption {
	return func(options *joinOptions) {
		options.state = state
	}
}

// WithGatewayForJoin sets the gateway on the options
func WithGatewayForJoin(gateway Gateway) JoinOption {
	return func(options *joinOptions) {
		options.gateway = gateway
	}
}

// WithNodeConfigSchemaForJoin sets the nodeConfigSchema on the options
func WithNodeConfigSchemaForJoin(nodeConfigSchema config.Schema) JoinOption {
	return func(options *joinOptions) {
		options.nodeConfigSchema = nodeConfigSchema
	}
}

// WithConfigForJoin sets the config on the options
func WithConfigForJoin(config NodeConfigProvider) JoinOption {
	return func(options *joinOptions) {
		options.config = config
	}
}

// WithFileSystemForJoin sets the fileSystem on the options
func WithFileSystemForJoin(fileSystem fsys.FileSystem) JoinOption {
	return func(options *joinOptions) {
		options.fileSystem = fileSystem
	}
}

// WithLoggerForJoin sets the logger on the options
func WithLoggerForJoin(logger log.Logger) JoinOption {
	return func(options *joinOptions) {
		options.logger = logger
	}
}

// Create a options instance with default values.
func newJoinOptions() *joinOptions {
	return &joinOptions{
		config:     nodeConfigShim{},
		fileSystem: fsys.NewLocalFileSystem(false),
		logger:     log.NewNopLogger(),
	}
}

// Join makes a non-clustered node join an existing cluster.
//
// It's assumed that Accept() was previously called against the target node,
// which handed the raft server ID.
//
// The cert parameter must contain the keypair/CA material of the cluster being
// joined.
type Join struct {
	state            State
	gateway          Gateway
	nodeConfigSchema config.Schema
	config           NodeConfigProvider
	fileSystem       fsys.FileSystem
	logger           log.Logger
}

// NewJoin creates a Join with sane defaults.
func NewJoin(state State, gateway Gateway, nodeConfigSchema config.Schema, options ...JoinOption) *Join {
	opts := newJoinOptions()
	opts.state = state
	opts.gateway = gateway
	opts.nodeConfigSchema = nodeConfigSchema
	for _, option := range options {
		option(opts)
	}

	return &Join{
		state:            opts.state,
		gateway:          opts.gateway,
		config:           opts.config,
		nodeConfigSchema: opts.nodeConfigSchema,
		fileSystem:       opts.fileSystem,
		logger:           opts.logger,
	}
}

// Run executes the join and returns an error if something goes wrong
func (j *Join) Run(certInfo *cert.Info, name string, nodes []db.RaftNode) error {
	// Check parameters
	if name == "" {
		return errors.Errorf("node name must not be empty")
	}

	var address string
	if err := j.state.Node().Transaction(func(tx *db.NodeTx) error {
		// Fetch current network address and raft nodes
		config, err := j.config.ConfigLoad(tx, j.nodeConfigSchema)
		if err != nil {
			return errors.Wrap(err, "failed to fetch node configuration")
		}
		address, err = config.HTTPSAddress()
		if err != nil {
			return errors.Wrap(err, "failed to fetch node address")
		}

		// Make sure node-local database state is in order
		if err := membershipCheckNodeStateForBootstrapOrJoin(tx, address); err != nil {
			return errors.WithStack(err)
		}

		// Set the raft nodes list to the one that was returned by an Accept().
		err = tx.RaftNodesReplace(nodes)
		return errors.Wrap(err, "failed to set raft nodes")
	}); err != nil {
		return errors.WithStack(err)
	}

	// Get any outstanding operation, typically there will be just one, created
	// by the POST /cluster/nodes request which triggered this code.
	var operations []db.Operation
	if err := j.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		operations, err = tx.Operations()
		return errors.WithStack(err)
	}); err != nil {
		return errors.WithStack(err)
	}

	// Lock regular access to the cluster database since we don't want any
	// other database code to run while we're reconfiguring raft.
	if err := j.state.Cluster().EnterExclusive(); err != nil {
		return errors.Wrap(err, "failed to acquire cluster database lock")
	}

	// Shutdown the gateway and wipe any raft data. This will trash any
	// gRPC SQL connection against our in-memory dqlite driver and shutdown
	// the associated raft instance.
	if err := j.gateway.Shutdown(); err != nil {
		return errors.Wrap(err, "failed to shutdown gRPC SQL gateway")
	}
	if err := j.fileSystem.RemoveAll(j.state.OS().GlobalDatabaseDir()); err != nil {
		return errors.Wrap(err, "failed to remove existing raft data")
	}

	// Re-initialize the gateway. This will create a new raft factory an
	// dqlite driver instance, which will be exposed over gRPC by the
	// gateway handlers.
	if err := j.gateway.Init(certInfo); err != nil {
		return errors.Wrap(err, "failed to re-initialize gRPC SQL gateway")
	}

	// If we are listed among the database nodes, join the raft cluster.
	var id, target string
	for _, node := range nodes {
		if node.Address == address {
			id = strconv.Itoa(int(node.ID))
		} else {
			target = node.Address
		}
	}
	if id != "" {
		level.Info(j.logger).Log(
			"msg", "Joining dqlite raft cluster",
			"id", id,
			"address", address,
			"target", target,
		)
		changer := j.gateway.Raft().MembershipChanger()
		if err := changer.Join(raft.ServerID(id), raft.ServerAddress(target), 5*time.Second); err != nil {
			return errors.WithStack(err)
		}
	} else {
		level.Info(j.logger).Log("msg", "Joining cluster as non-database node")
	}

	// Make sure we can actually connect to the cluster database through
	// the network endpoint. This also releases the previously acquired
	// lock and makes the Go SQL pooling system invalidate the old
	// connection, so new queries will be executed over the new gRPC
	// network connection. Also, update the storage_pools and networks
	// tables with our local configuration.
	level.Info(j.logger).Log("msg", "Migrate local data to cluster database")
	if err := j.state.Cluster().ExitExclusive(j.exitExclusive(address, operations)); err != nil {
		return errors.Wrap(err, "cluster database initialization failed")
	}
	return nil
}

func (j *Join) exitExclusive(address string, operations []db.Operation) func(*db.ClusterTx) error {
	return func(tx *db.ClusterTx) error {
		node, err := tx.NodePendingByAddress(address)
		if err != nil {
			return errors.Wrap(err, "failed to get ID of joining node")
		}
		j.state.Cluster().NodeID(node.ID)
		tx.NodeID(node.ID)

		// Migrate outstanding operations.
		for _, op := range operations {
			if _, err := tx.OperationAdd(op.UUID, op.Type); err != nil {
				return errors.Wrapf(err, "failed to migrate operation %q", op.UUID)
			}
		}

		// Remove the pending flag for ourselves notifications.
		err = tx.NodePending(node.ID, false)
		return errors.Wrapf(err, "failed to unmark the node as pending")
	}
}
