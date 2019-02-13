package membership

import (
	"fmt"

	"github.com/spoke-d/thermionic/internal/clock"
	clusterconfig "github.com/spoke-d/thermionic/internal/cluster/config"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// RebalanceOption to be passed to NewRebalance to customize the resulting
// instance.
type RebalanceOption func(*rebalanceOptions)

type rebalanceOptions struct {
	state   State
	gateway Gateway
	config  ClusterConfigProvider
	schema  config.Schema
	logger  log.Logger
	clock   clock.Clock
}

// WithStateForRebalance sets the state on the options
func WithStateForRebalance(state State) RebalanceOption {
	return func(options *rebalanceOptions) {
		options.state = state
	}
}

// WithGatewayForRebalance sets the gateway on the options
func WithGatewayForRebalance(gateway Gateway) RebalanceOption {
	return func(options *rebalanceOptions) {
		options.gateway = gateway
	}
}

// WithConfigForRebalance sets the config on the options
func WithConfigForRebalance(config ClusterConfigProvider) RebalanceOption {
	return func(options *rebalanceOptions) {
		options.config = config
	}
}

// WithSchemaForRebalance sets the schema on the options
func WithSchemaForRebalance(schema config.Schema) RebalanceOption {
	return func(options *rebalanceOptions) {
		options.schema = schema
	}
}

// WithLoggerForRebalance sets the logger on the options
func WithLoggerForRebalance(logger log.Logger) RebalanceOption {
	return func(options *rebalanceOptions) {
		options.logger = logger
	}
}

// WithClockForRebalance sets the clock on the options
func WithClockForRebalance(clock clock.Clock) RebalanceOption {
	return func(options *rebalanceOptions) {
		options.clock = clock
	}
}

// Create a options instance with default values.
func newRebalanceOptions() *rebalanceOptions {
	return &rebalanceOptions{
		config: clusterConfigShim{},
		logger: log.NewNopLogger(),
		clock:  clock.New(),
	}
}

// Rebalance the raft cluster, trying to see if we have a spare online node
// that we can promote to database node if we are below membershipQuorumRaftNodes.
//
// If there's such spare node, return its address as well as the new list of
// raft nodes.
type Rebalance struct {
	state   State
	gateway Gateway
	config  ClusterConfigProvider
	schema  config.Schema
	logger  log.Logger
	clock   clock.Clock
}

// NewRebalance creates a Rebalance with sane defaults
func NewRebalance(state State, gateway Gateway, schema config.Schema, options ...RebalanceOption) *Rebalance {
	opts := newRebalanceOptions()
	opts.state = state
	opts.gateway = gateway
	opts.schema = schema
	for _, option := range options {
		option(opts)
	}

	return &Rebalance{
		state:   opts.state,
		gateway: opts.gateway,
		schema:  opts.schema,
		config:  opts.config,
		logger:  opts.logger,
		clock:   opts.clock,
	}
}

// Run executes the rebalance and returns an error if something goes wrong
func (r *Rebalance) Run() (string, []db.RaftNode, error) {
	// First get the current raft members, since this method should be
	// called after a node has left.
	currentRaftNodes, err := r.gateway.RaftNodes()
	if err != nil {
		return "", nil, errors.Wrap(err, "failed to get current raft nodes")
	}
	if len(currentRaftNodes) >= membershipQuorumRaftNodes {
		// We're already at full capacity.
		return "", nil, nil
	}

	currentRaftAddresses := make([]string, len(currentRaftNodes))
	for i, node := range currentRaftNodes {
		currentRaftAddresses[i] = node.Address
	}

	// Check if we have a spare node that we can turn into a database one.
	var address string
	if err := r.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		config, err := r.config.ConfigLoad(tx, r.schema)
		if err != nil {
			return errors.Wrap(err, "failed load cluster configuration")
		}
		threshold, err := config.OfflineThreshold()
		if err != nil {
			return errors.Wrap(err, "failed to get config threshold")
		}
		nodes, err := tx.Nodes()
		if err != nil {
			return errors.Wrap(err, "failed to get cluster nodes")
		}

		// Find a node that is not part of the raft cluster yet.
		for _, node := range nodes {
			if contains(node.Address, currentRaftAddresses) {
				continue
			}
			if node.IsOffline(r.clock, threshold) {
				continue
			}
			level.Debug(r.logger).Log("msg", fmt.Sprintf("Found spare node %s (%s) to be promoted as database node", node.Name, node.Address))
			address = node.Address
			break
		}
		return nil
	}); err != nil {
		return "", nil, errors.WithStack(err)
	}

	if address == "" {
		// No node to promote
		return "", nil, nil
	}

	// Update the local raft_table adding the new member and building a new
	// list.
	updatedRafNodes := currentRaftNodes
	if err := r.gateway.DB().Transaction(func(tx *db.NodeTx) error {
		id, err := tx.RaftNodeAdd(address)
		if err != nil {
			return errors.Wrap(err, "failed to add new raft node")
		}
		updatedRafNodes = append(updatedRafNodes, db.RaftNode{
			ID:      id,
			Address: address,
		})
		err = tx.RaftNodesReplace(updatedRafNodes)
		return errors.Wrap(err, "failed to update raft nodes")

	}); err != nil {
		return "", nil, errors.WithStack(err)
	}
	return address, updatedRafNodes, nil
}

func contains(address string, addresses []string) bool {
	for _, v := range addresses {
		if v == address {
			return true
		}
	}
	return false
}

type clusterConfigShim struct{}

func (clusterConfigShim) ConfigLoad(tx *db.ClusterTx, schema config.Schema) (*clusterconfig.Config, error) {
	return clusterconfig.Load(tx, schema)
}
