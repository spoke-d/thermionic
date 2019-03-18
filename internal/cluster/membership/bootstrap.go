package membership

import (
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/node"
)

// BootstrapOption to be passed to NewBootstrap to customize the resulting
// instance.
type BootstrapOption func(*bootstrapOptions)

type bootstrapOptions struct {
	certInfo         *cert.Info
	nodeConfigSchema config.Schema
	state            State
	gateway          Gateway
	config           NodeConfigProvider
	fileSystem       fsys.FileSystem
}

// WithCertForBootstrap sets the certInfo on the options
func WithCertForBootstrap(certInfo *cert.Info) BootstrapOption {
	return func(options *bootstrapOptions) {
		options.certInfo = certInfo
	}
}

// WithStateForBootstrap sets the state on the options
func WithStateForBootstrap(state State) BootstrapOption {
	return func(options *bootstrapOptions) {
		options.state = state
	}
}

// WithGatewayForBootstrap sets the gateway on the options
func WithGatewayForBootstrap(gateway Gateway) BootstrapOption {
	return func(options *bootstrapOptions) {
		options.gateway = gateway
	}
}

// WithConfigForBootstrap sets the config on the options
func WithConfigForBootstrap(config NodeConfigProvider) BootstrapOption {
	return func(options *bootstrapOptions) {
		options.config = config
	}
}

// WithNodeConfigSchemaForBootstrap sets the nodeConfigSchema on the options
func WithNodeConfigSchemaForBootstrap(nodeConfigSchema config.Schema) BootstrapOption {
	return func(options *bootstrapOptions) {
		options.nodeConfigSchema = nodeConfigSchema
	}
}

// WithFileSystemForBootstrap sets the fileSystem on the options
func WithFileSystemForBootstrap(fileSystem fsys.FileSystem) BootstrapOption {
	return func(options *bootstrapOptions) {
		options.fileSystem = fileSystem
	}
}

// Create a options instance with default values.
func newBootstrapOptions() *bootstrapOptions {
	return &bootstrapOptions{
		config:     nodeConfigShim{},
		fileSystem: fsys.NewLocalFileSystem(false),
	}
}

// Bootstrap turns a non-clustered instance into the first (and leader)
// node of a new cluster.
//
// This instance must already have its core.https_address set and be listening
// on the associated network address.
type Bootstrap struct {
	state            State
	certInfo         *cert.Info
	nodeConfigSchema config.Schema
	gateway          Gateway
	config           NodeConfigProvider
	fileSystem       fsys.FileSystem
}

// NewBootstrap creates a Bootstrap with sane defaults.
func NewBootstrap(
	state State,
	gateway Gateway,
	certInfo *cert.Info,
	nodeConfigSchema config.Schema,
	options ...BootstrapOption,
) *Bootstrap {
	opts := newBootstrapOptions()
	opts.state = state
	opts.gateway = gateway
	opts.certInfo = certInfo
	opts.nodeConfigSchema = nodeConfigSchema
	for _, option := range options {
		option(opts)
	}

	return &Bootstrap{
		state:            opts.state,
		gateway:          opts.gateway,
		config:           opts.config,
		fileSystem:       opts.fileSystem,
		certInfo:         opts.certInfo,
		nodeConfigSchema: opts.nodeConfigSchema,
	}
}

// Run executes the bootstrap and returns an error if something goes wrong
func (b *Bootstrap) Run(name string) error {
	// Check parameters
	if name == "" {
		return errors.Errorf("node name must not be empty")
	}

	varDir := b.state.OS().VarDir()
	if err := membershipCheckNoLeftOverClusterCert(b.fileSystem, varDir); err != nil {
		return errors.WithStack(err)
	}

	var address string
	if err := b.state.Node().Transaction(func(tx *db.NodeTx) error {
		// Fetch current network address and raft nodes
		config, err := b.config.ConfigLoad(tx, b.nodeConfigSchema)
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

		// Add ourselves as first raft node
		if err := tx.RaftNodeFirst(address); err != nil {
			return errors.Wrap(err, "failed to insert first raft node")
		}

		return nil
	}); err != nil {
		return errors.WithStack(err)
	}

	// Update our own entry in the nodes table.
	if err := b.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		// Make sure cluster state is in order
		if err := membershipCheckClusterStateForBootstrapOrJoin(tx); err != nil {
			return errors.WithStack(err)
		}

		// Add ourselves to the node table.
		err := tx.NodeUpdate(1, name, address)
		return errors.Wrap(err, "failed to update cluster node")
	}); err != nil {
		return errors.WithStack(err)
	}

	// Shutdown the gateway. This will trash any connection
	// against our in-memory dqlite driver and shutdown the associated raft
	// instance. We also lock regular access to the cluster database since
	// we don't want any other database code to run while we're
	// reconfiguring raft.
	if err := b.state.Cluster().EnterExclusive(); err != nil {
		return errors.Wrap(err, "failed to acquire cluster database lock")
	}
	if err := b.gateway.Shutdown(); err != nil {
		return errors.Wrap(err, "failed to shutdown gateway")
	}

	// Re-initialize the gateway. This will create a new raft factory an
	// dqlite driver instance, which will be exposed over gRPC by the
	// gateway handlers.
	if err := b.gateway.Init(b.certInfo); err != nil {
		return errors.Wrap(err, "failed to re-initialize gateway")
	}

	if err := b.gateway.WaitLeadership(); err != nil {
		return err
	}

	// The cluster certificates are symlinks against the regular node
	// certificate.
	for _, ext := range []string{".crt", ".key", ".ca"} {
		if ext == ".ca" && !b.fileSystem.Exists(filepath.Join(varDir, "server.ca")) {
			continue
		}
		path := filepath.Join(varDir, "cluster"+ext)
		err := b.fileSystem.Symlink("server"+ext, path)
		if err != nil {
			return errors.Wrap(err, "failed to create cluster cert symlink")
		}
	}

	// Make sure we can actually connect to the cluster database through
	// the network endpoint. This also releases the previously acquired
	// lock and makes the Go SQL pooling system invalidate the old
	// connection, so new queries will be executed over the new gRPC
	// network connection.
	err := b.state.Cluster().ExitExclusive(func(tx *db.ClusterTx) error {
		_, err := tx.Nodes()
		return err
	})
	return errors.Wrap(err, "cluster database initialization failed")
}

// Check that there is no left-over cluster certificate in the var dir of
// this node.
func membershipCheckNoLeftOverClusterCert(fileSystem fsys.FileSystem, dir string) error {
	// Sanity check that there's no leftover cluster certificate
	for _, basename := range []string{"cluster.crt", "cluster.key", "cluster.ca"} {
		if fileSystem.Exists(filepath.Join(dir, basename)) {
			return errors.Errorf("inconsistent state: found leftover cluster certificate")
		}
	}
	return nil
}

// Check that node-related preconditions are met for bootstrapping or joining a
// cluster.
func membershipCheckNodeStateForBootstrapOrJoin(tx *db.NodeTx, address string) error {
	nodes, err := tx.RaftNodes()
	if err != nil {
		return errors.Wrap(err, "failed to fetch current raft nodes")
	}

	hasNetworkAddress := address != ""
	hasRaftNodes := len(nodes) > 0

	// Sanity check that we're not in an inconsistent situation, where no
	// network address is set, but still there are entries in the
	// raft_nodes table.
	if !hasNetworkAddress && hasRaftNodes {
		return errors.Errorf("inconsistent state: found leftover entries in raft_nodes")
	}
	if !hasNetworkAddress {
		return errors.Errorf("no core.https_address config is set on this node")
	}
	if hasRaftNodes {
		return errors.Errorf("the node is already part of a cluster")
	}
	return nil
}

// Check that cluster-related preconditions are met for bootstrapping or
// joining a cluster.
func membershipCheckClusterStateForBootstrapOrJoin(tx *db.ClusterTx) error {
	nodes, err := tx.Nodes()
	if err != nil {
		return errors.Wrap(err, "failed to fetch current cluster nodes")
	}
	if len(nodes) != 1 {
		return errors.Errorf("inconsistent state: found leftover entries in nodes")
	}
	return nil
}

type nodeConfigShim struct{}

func (nodeConfigShim) ConfigLoad(tx *db.NodeTx, configSchema config.Schema) (*node.Config, error) {
	return node.ConfigLoad(tx, configSchema)
}
