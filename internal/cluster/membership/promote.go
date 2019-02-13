package membership

import (
	"strconv"
	"time"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// PromoteOption to be passed to NewPromote to customize the resulting
// instance.
type PromoteOption func(*promoteOptions)

type promoteOptions struct {
	state      State
	gateway    Gateway
	logger     log.Logger
	fileSystem fsys.FileSystem
}

// WithStateForPromote sets the state on the options
func WithStateForPromote(state State) PromoteOption {
	return func(options *promoteOptions) {
		options.state = state
	}
}

// WithGatewayForPromote sets the gateway on the options
func WithGatewayForPromote(gateway Gateway) PromoteOption {
	return func(options *promoteOptions) {
		options.gateway = gateway
	}
}

// WithLoggerForPromote sets the logger on the options
func WithLoggerForPromote(logger log.Logger) PromoteOption {
	return func(options *promoteOptions) {
		options.logger = logger
	}
}

// WithFileSystemForPromote sets the fileSystem on the options
func WithFileSystemForPromote(fileSystem fsys.FileSystem) PromoteOption {
	return func(options *promoteOptions) {
		options.fileSystem = fileSystem
	}
}

// Create a options instance with default values.
func newPromoteOptions() *promoteOptions {
	return &promoteOptions{
		logger:     log.NewNopLogger(),
		fileSystem: fsys.NewLocalFileSystem(false),
	}
}

// Promote makes a node which is not a database node, become part of the raft
// cluster.
type Promote struct {
	state      State
	gateway    Gateway
	fileSystem fsys.FileSystem
	logger     log.Logger
}

// NewPromote creates a Promote with sane defaults.
func NewPromote(state State, gateway Gateway, options ...PromoteOption) *Promote {
	opts := newPromoteOptions()
	opts.state = state
	opts.gateway = gateway
	for _, option := range options {
		option(opts)
	}

	return &Promote{
		state:      opts.state,
		gateway:    opts.gateway,
		fileSystem: opts.fileSystem,
		logger:     opts.logger,
	}
}

// Run executes the promote and returns an error if something goes wrong
func (p *Promote) Run(certInfo *cert.Info, nodes []db.RaftNode) error {
	level.Info(p.logger).Log("msg", "Promote node to database node")

	// Sanity check that this is not already a database node
	if p.gateway.IsDatabaseNode() {
		return errors.Errorf("node is already a database node")
	}

	// Figure out our own address.
	var address string
	if err := p.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		address, err = tx.NodeAddress()
		return errors.Wrap(err, "failed to fetch the address of this node")
	}); err != nil {
		return errors.WithStack(err)
	}

	// Sanity check that we actually have an address.
	if address == "" {
		return errors.Errorf("node is not exposed on the network")
	}

	// Figure out our raft node ID, and an existing target raft node that
	// we'll contact to add ourselves as member.
	var id, target string
	for _, node := range nodes {
		if node.Address == address {
			id = strconv.Itoa(int(node.ID))
		} else {
			target = node.Address
		}
	}

	// Sanity check that our address was actually included in the given
	// list of raft nodes.
	if id == "" {
		return errors.Errorf("this node is not included in the given list of database nodes")
	}

	// Replace our local list of raft nodes with the given one (which
	// includes ourselves). This will make the gateway start a raft node
	// when restarted.
	if err := p.state.Node().Transaction(func(tx *db.NodeTx) error {
		err := tx.RaftNodesReplace(nodes)
		return errors.Wrap(err, "failed to set raft nodes")
	}); err != nil {
		return errors.WithStack(err)
	}

	// Lock regular access to the cluster database since we don't want any
	// other database code to run while we're reconfiguring raft.
	if err := p.state.Cluster().EnterExclusive(); err != nil {
		return errors.Wrap(err, "failed to acquire cluster database lock")
	}

	// Wipe all existing raft data, for good measure (perhaps they were
	// somehow leftover).
	if err := p.fileSystem.RemoveAll(p.state.OS().GlobalDatabaseDir()); err != nil {
		return errors.Wrap(err, "failed to remove existing raft data")
	}

	// Re-initialize the gateway. This will create a new raft factory an
	// dqlite driver instance, which will be exposed over gRPC by the
	// gateway handlers.
	if err := p.gateway.Init(certInfo); err != nil {
		return errors.Wrap(err, "failed to re-initialize gRPC SQL gateway")
	}

	level.Info(p.logger).Log(
		"msg", "Promoting dqlite raft cluster",
		"id", id,
		"address", address,
		"target", target,
	)

	changer := p.gateway.Raft().MembershipChanger()
	if err := changer.Join(raft.ServerID(id), raft.ServerAddress(target), 5*time.Second); err != nil {
		return errors.WithStack(err)
	}

	// Unlock regular access to our cluster database, and make sure our
	// gateway still works correctly.
	if err := p.state.Cluster().ExitExclusive(func(tx *db.ClusterTx) error {
		_, err := tx.Nodes()
		return errors.WithStack(err)
	}); err != nil {
		return errors.Wrap(err, "cluster database initialization failed")
	}
	return nil
}
