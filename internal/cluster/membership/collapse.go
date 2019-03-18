package membership

import (
	"fmt"
	"path/filepath"
	"time"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	querycluster "github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/node"
)

// CollapseOption to be passed to NewCollapse to customize the resulting
// instance.
type CollapseOption func(*collapseOptions)

type collapseOptions struct {
	nodeConfigSchema config.Schema
	state            State
	gateway          Gateway
	endpoints        Endpoints
	config           NodeConfigProvider
	fileSystem       fsys.FileSystem
	logger           log.Logger
}

// WithStateForCollapse sets the state on the options
func WithStateForCollapse(state State) CollapseOption {
	return func(options *collapseOptions) {
		options.state = state
	}
}

// WithGatewayForCollapse sets the gateway on the options
func WithGatewayForCollapse(gateway Gateway) CollapseOption {
	return func(options *collapseOptions) {
		options.gateway = gateway
	}
}

// WithEndpointsForCollapse sets the endpoints on the options
func WithEndpointsForCollapse(endpoints Endpoints) CollapseOption {
	return func(options *collapseOptions) {
		options.endpoints = endpoints
	}
}

// WithConfigForCollapse sets the config on the options
func WithConfigForCollapse(config NodeConfigProvider) CollapseOption {
	return func(options *collapseOptions) {
		options.config = config
	}
}

// WithNodeConfigSchemaForCollapse sets the nodeConfigSchema on the options
func WithNodeConfigSchemaForCollapse(nodeConfigSchema config.Schema) CollapseOption {
	return func(options *collapseOptions) {
		options.nodeConfigSchema = nodeConfigSchema
	}
}

// WithFileSystemForCollapse sets the fileSystem on the options
func WithFileSystemForCollapse(fileSystem fsys.FileSystem) CollapseOption {
	return func(options *collapseOptions) {
		options.fileSystem = fileSystem
	}
}

// WithLoggerForCollapse sets the logger on the options
func WithLoggerForCollapse(logger log.Logger) CollapseOption {
	return func(options *collapseOptions) {
		options.logger = logger
	}
}

// Create a options instance with default values.
func newCollapseOptions() *collapseOptions {
	return &collapseOptions{
		config:     nodeConfigShim{},
		fileSystem: fsys.NewLocalFileSystem(false),
		logger:     log.NewNopLogger(),
	}
}

// Collapse turns a clustered instance into a non-clustered instance, by
// collapsing the cluster.
//
// This instance must already have its core.https_address set and be listening
// on the associated network address.
type Collapse struct {
	state            State
	nodeConfigSchema config.Schema
	gateway          Gateway
	endpoints        Endpoints
	config           NodeConfigProvider
	fileSystem       fsys.FileSystem
	logger           log.Logger
}

// NewCollapse creates a Collapse with sane defaults.
func NewCollapse(
	state State,
	gateway Gateway,
	endpoints Endpoints,
	nodeConfigSchema config.Schema,
	options ...CollapseOption,
) *Collapse {
	opts := newCollapseOptions()
	opts.state = state
	opts.gateway = gateway
	opts.endpoints = endpoints
	opts.nodeConfigSchema = nodeConfigSchema
	for _, option := range options {
		option(opts)
	}

	return &Collapse{
		state:            opts.state,
		gateway:          opts.gateway,
		endpoints:        opts.endpoints,
		config:           opts.config,
		fileSystem:       opts.fileSystem,
		nodeConfigSchema: opts.nodeConfigSchema,
	}
}

// Run executes the collapse and returns an error if something goes wrong
func (c *Collapse) Run(apiExtensions []string) (Cluster, error) {
	if err := c.state.Cluster().Close(); err != nil {
		return nil, errors.WithStack(err)
	}

	varDir := c.state.OS().VarDir()

	// Update our TLS configuration using our original certificate.
	for _, suffix := range []string{".crt", ".key", ".ca"} {
		path := filepath.Join(varDir, fmt.Sprintf("cluster%s", suffix))
		if !c.fileSystem.Exists(path) {
			continue
		}
		if err := c.fileSystem.Remove(path); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	certInfo, err := cert.LoadCert(
		varDir,
		cert.WithFileSystem(c.fileSystem),
		cert.WithOS(c.state.OS()),
		cert.WithLogger(log.WithPrefix(c.logger, "component", "cert")),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := c.endpoints.NetworkUpdateCert(certInfo); err != nil {
		return nil, errors.WithStack(err)
	}

	if err := c.gateway.Reset(certInfo); err != nil {
		return nil, errors.WithStack(err)
	}

	// Re-open the cluster database
	address, err := node.HTTPSAddress(c.state.Node(), c.nodeConfigSchema)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	dir := filepath.Join(varDir, "database")
	store := c.gateway.ServerStore()

	queryCluster := querycluster.New(
		querycluster.NewBasicAPIExtensions(len(apiExtensions)),
		querycluster.NewSchema(c.fileSystem),
	)

	dbCluster := db.NewCluster(
		queryCluster,
		db.WithFileSystemForCluster(c.fileSystem),
		db.WithLoggerForCluster(log.WithPrefix(c.logger, "component", "cluster")),
	)
	if err := dbCluster.Open(
		store, address, dir, 36*time.Hour,
		dqlite.WithDialFunc(c.gateway.DialFunc()),
		dqlite.WithContext(c.gateway.Context()),
		dqlite.WithConnectionTimeout(10*time.Second),
		dqlite.WithLogFunc(cluster.DqliteLog(c.logger)),
	); err != nil {
		return nil, errors.WithStack(err)
	}

	return dbCluster, nil
}
