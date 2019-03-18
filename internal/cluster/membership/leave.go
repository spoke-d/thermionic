package membership

import (
	"fmt"
	"strconv"
	"time"

	rafthttp "github.com/CanonicalLtd/raft-http"
	raftmembership "github.com/CanonicalLtd/raft-membership"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	clusterraft "github.com/spoke-d/thermionic/internal/cluster/raft"
	"github.com/spoke-d/thermionic/internal/db"
)

// LeaveOption to be passed to NewLeave to customize the resulting
// instance.
type LeaveOption func(*leaveOptions)

type leaveOptions struct {
	state          State
	gateway        Gateway
	membership     Membership
	dialerProvider DialerProvider
	logger         log.Logger
}

// WithStateForLeave sets the state on the options
func WithStateForLeave(state State) LeaveOption {
	return func(options *leaveOptions) {
		options.state = state
	}
}

// WithGatewayForLeave sets the gateway on the options
func WithGatewayForLeave(gateway Gateway) LeaveOption {
	return func(options *leaveOptions) {
		options.gateway = gateway
	}
}

// WithMembershipForLeave sets the membership on the options
func WithMembershipForLeave(membership Membership) LeaveOption {
	return func(options *leaveOptions) {
		options.membership = membership
	}
}

// WithDialerProviderForLeave sets the dialerProvider on the options
func WithDialerProviderForLeave(dialerProvider DialerProvider) LeaveOption {
	return func(options *leaveOptions) {
		options.dialerProvider = dialerProvider
	}
}

// WithLoggerForLeave sets the logger on the options
func WithLoggerForLeave(logger log.Logger) LeaveOption {
	return func(options *leaveOptions) {
		options.logger = logger
	}
}

// Create a options instance with default values.
func newLeaveOptions() *leaveOptions {
	return &leaveOptions{
		membership:     NewRequestChanger(LeaveRequest),
		dialerProvider: dialerProviderShim{},
		logger:         log.NewNopLogger(),
	}
}

// DialerProvider provides a way to create a dialer for a raft cluster
type DialerProvider interface {
	// Dial creates a rafthttp.Dial function that connects over TLS using the given
	// cluster (and optionally CA) certificate both as client and remote
	// certificate.
	Dial(certInfo *cert.Info) (rafthttp.Dial, error)
}

// Membership defines an interface for change membership of a cluster
type Membership interface {

	// Change can be used to join or leave a cluster over HTTP.
	Change(
		dial rafthttp.Dial,
		id raft.ServerID,
		address, target string,
		timeout time.Duration) error
}

// Leave a cluster.
//
// If the force flag is true, the node will leave even if it still has
// containers and images.
//
// The node will only leave the raft cluster, and won't be removed from the
// database. That's done by Purge().
//
// Upon success, return the address of the leaving node.
type Leave struct {
	state          State
	gateway        Gateway
	membership     Membership
	dialerProvider DialerProvider
	logger         log.Logger
}

// NewLeave creates a Leave with sane defaults.
func NewLeave(state State, gateway Gateway, options ...LeaveOption) *Leave {
	opts := newLeaveOptions()
	opts.state = state
	opts.gateway = gateway
	for _, option := range options {
		option(opts)
	}

	return &Leave{
		state:          opts.state,
		gateway:        opts.gateway,
		membership:     opts.membership,
		dialerProvider: opts.dialerProvider,
		logger:         opts.logger,
	}
}

// Run executes the promote and returns an error if something goes wrong
func (l *Leave) Run(name string, force bool) (string, error) {
	level.Info(l.logger).Log("msg", fmt.Sprintf("Make node %s leave the cluster", name))

	// Check if the node can be deleted and track its address.
	var address string
	if err := l.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		// Get the node (if it doesn't exists an error is returned).
		node, err := tx.NodeByName(name)
		if err != nil {
			return errors.WithStack(err)
		}

		// Check that the node is eligible for leaving.
		if !force {
			err := membershipCheckClusterStateForLeave(tx, node.ID)
			if err != nil {
				return err
			}
		}
		address = node.Address
		return nil
	}); err != nil {
		return "", errors.WithStack(err)
	}

	// If the node is a database node, leave the raft cluster too.
	var raftNodes []db.RaftNode // Current raft nodes
	raftNodeRemoveIndex := -1   // Index of the raft node to remove, if any.
	if err := l.state.Node().Transaction(func(tx *db.NodeTx) error {
		var err error
		if raftNodes, err = tx.RaftNodes(); err != nil {
			return errors.Wrap(err, "failed to get current database nodes")
		}
		for i, node := range raftNodes {
			if node.Address == address {
				raftNodeRemoveIndex = i
				break
			}
		}
		return nil
	}); err != nil {
		return "", errors.WithStack(err)
	}

	if raftNodeRemoveIndex == -1 {
		// The node was not part of the raft cluster, nothing left to
		// do.
		return address, nil
	}

	id := strconv.Itoa(int(raftNodes[raftNodeRemoveIndex].ID))
	// Get the address of another database node,
	target := raftNodes[(raftNodeRemoveIndex+1)%len(raftNodes)].Address

	level.Info(l.logger).Log(
		"msg", "Remove node from dqlite raft cluster",
		"id", id,
		"address", address,
		"target", target,
	)

	dial, err := l.dialerProvider.Dial(l.gateway.Cert())
	if err != nil {
		return "", errors.WithStack(err)
	}

	err = l.membership.Change(
		dial,
		raft.ServerID(id), address, target,
		5*time.Second,
	)
	return address, errors.WithStack(err)
}

// Check that cluster-related preconditions are met for leaving a cluster.
func membershipCheckClusterStateForLeave(tx *db.ClusterTx, nodeID int64) error {
	// Check that it has no containers or images.
	message, err := tx.NodeIsEmpty(nodeID)
	if err != nil {
		return errors.WithStack(err)
	}
	if message != "" {
		return errors.Errorf(message)
	}

	// Check that it's not the last node.
	nodes, err := tx.Nodes()
	if err != nil {
		return errors.WithStack(err)
	}
	if len(nodes) == 1 {
		return errors.Errorf("node is the only node in the cluster")
	}
	return nil
}

// Request defines a type of request for a membership change
type Request int

const (
	// JoinRequest defines a request for a membership change
	JoinRequest Request = iota

	// LeaveRequest defines a request for a membership change
	LeaveRequest Request = iota
)

func (r Request) raftStatus() raftmembership.ChangeRequestKind {
	switch r {
	case JoinRequest:
		return raftmembership.JoinRequest
	case LeaveRequest:
		return raftmembership.LeaveRequest
	default:
		return -1
	}
}

// RequestChanger represents a way to propagate requests for changing of a
// membership with in the underlying raft
type RequestChanger struct {
	request Request
}

// NewRequestChanger creates a new RequestChanger with sane defaults
func NewRequestChanger(request Request) RequestChanger {
	return RequestChanger{
		request: request,
	}
}

// Change requests a membership change for the internal raft cluster.
func (r RequestChanger) Change(
	dial rafthttp.Dial,
	id raft.ServerID,
	address, target string,
	timeout time.Duration) error {
	return rafthttp.ChangeMembership(
		r.request.raftStatus(),
		clusterraft.Endpoint,
		dial,
		id,
		address,
		target,
		timeout,
	)
}

type dialerProviderShim struct{}

func (dialerProviderShim) Dial(certInfo *cert.Info) (rafthttp.Dial, error) {
	return clusterraft.NewDialer(certInfo)
}
