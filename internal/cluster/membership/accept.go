package membership

import (
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db"
)

// AcceptOption to be passed to NewAccept to customize the resulting
// instance.
type AcceptOption func(*acceptOptions)

type acceptOptions struct {
	state   State
	gateway Gateway
}

// WithStateForAccept sets the state on the options
func WithStateForAccept(state State) AcceptOption {
	return func(options *acceptOptions) {
		options.state = state
	}
}

// WithGatewayForAccept sets the gateway on the options
func WithGatewayForAccept(gateway Gateway) AcceptOption {
	return func(options *acceptOptions) {
		options.gateway = gateway
	}
}

// Create a options instance with default values.
func newAcceptOptions() *acceptOptions {
	return &acceptOptions{}
}

// Accept a new node and add it to the cluster.
//
// This instance must already be clustered.
//
// Return an updated list raft database nodes (possibly including the newly
// accepted node).
type Accept struct {
	state   State
	gateway Gateway
}

// NewAccept creates a Accept with sane defaults.
func NewAccept(state State, gateway Gateway, options ...AcceptOption) *Accept {
	opts := newAcceptOptions()
	opts.state = state
	opts.gateway = gateway
	for _, option := range options {
		option(opts)
	}

	return &Accept{
		state:   opts.state,
		gateway: opts.gateway,
	}
}

// Run executes the accept and returns an error if something goes wrong
func (a *Accept) Run(name, address string, schema, api int) ([]db.RaftNode, error) {
	// Check parameters
	if name == "" {
		return nil, errors.Errorf("node name must not be empty")
	}
	if address == "" {
		return nil, errors.Errorf("node address must not be empty")
	}

	if err := a.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		// Check that the node can be accepted with these parameters.
		if err := membershipCheckClusterStateForAccept(tx, name, address, schema, api); err != nil {
			return errors.WithStack(err)
		}

		// Add a new node
		id, err := tx.NodeAdd(name, address, schema, api)
		if err != nil {
			return errors.Wrap(err, "failed to insert new node")
		}

		// Mark the node as pending, so will be skipped when performing
		// heartbeats or sending cluster notifications
		err = tx.NodePending(id, true)
		return errors.Wrap(err, "failed to mark new node as pending")
	}); err != nil {
		return nil, errors.WithStack(err)
	}

	// Possibly insert the node into the raft_nodes table (if we have less than
	// the minimum quorum node).
	nodes, err := a.gateway.RaftNodes()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get raft nodes from the log")
	}
	if len(nodes) < membershipQuorumRaftNodes {
		if err := a.state.Node().Transaction(func(tx *db.NodeTx) error {
			id, err := tx.RaftNodeAdd(address)
			if err != nil {
				return errors.WithStack(err)
			}
			nodes = append(nodes, db.RaftNode{
				ID:      id,
				Address: address,
			})
			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "failed to insert new node into raft_node")
		}
	}
	return nodes, nil
}

// Check that cluster-related preconditions are met for accepting a new node.
func membershipCheckClusterStateForAccept(tx *db.ClusterTx, name string, address string, schema int, api int) error {
	nodes, err := tx.Nodes()
	if err != nil {
		return errors.Wrap(err, "failed to fetch current cluster nodes")
	}
	if len(nodes) == 1 && nodes[0].Address == "0.0.0.0" {
		return errors.Errorf("clustering not enabled")
	}

	for _, node := range nodes {
		if node.Name == name {
			return errors.Errorf("cluster already has node with name %s", name)
		}
		if node.Address == address {
			return errors.Errorf("cluster already has node with address %s", address)
		}
		if node.Schema != schema {
			return errors.Errorf("schema version mismatch: cluster has %d", node.Schema)
		}
		if node.APIExtensions != api {
			return errors.Errorf("api version mismatch: cluster has %d", node.APIExtensions)
		}
	}

	return nil
}
