package membership

import (
	"fmt"
	"time"

	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/version"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
)

// ListOption to be passed to NewList to customize the resulting
// instance.
type ListOption func(*listOptions)

type listOptions struct {
	state  State
	clock  clock.Clock
	logger log.Logger
}

// WithStateForList sets the state on the options
func WithStateForList(state State) ListOption {
	return func(options *listOptions) {
		options.state = state
	}
}

// WithClockForList sets the clock on the options
func WithClockForList(clock clock.Clock) ListOption {
	return func(options *listOptions) {
		options.clock = clock
	}
}

// WithLoggerForList sets the logger on the options
func WithLoggerForList(logger log.Logger) ListOption {
	return func(options *listOptions) {
		options.logger = logger
	}
}

// Create a options instance with default values.
func newListOptions() *listOptions {
	return &listOptions{
		clock:  clock.New(),
		logger: log.NewNopLogger(),
	}
}

const (
	// StatusOnline represents a node online
	StatusOnline = "online"
	// StatusOffline represents a node offline
	StatusOffline = "offline"
	// StatusBroken represents a node broken state
	StatusBroken = "broken"
	// StatusBlocked represents a node blocked state
	StatusBlocked = "blocked"
)

// ClusterMember represents a node in the cluster.
type ClusterMember struct {
	ServerName string
	URL        string
	Database   bool
	Status     string
	Message    string
}

// List the nodes of the cluster.
type List struct {
	state  State
	clock  clock.Clock
	logger log.Logger
}

// NewList creates a List with sane defaults.
func NewList(state State, options ...ListOption) *List {
	opts := newListOptions()
	opts.state = state
	for _, option := range options {
		option(opts)
	}

	return &List{
		state:  opts.state,
		clock:  opts.clock,
		logger: opts.logger,
	}
}

// Run executes the promote and returns an error if something goes wrong
func (l *List) Run() ([]ClusterMember, error) {
	var addresses []string
	if err := l.state.Node().Transaction(func(tx *db.NodeTx) error {
		nodes, err := tx.RaftNodes()
		if err != nil {
			return errors.Wrap(err, "failed to fetch current raft nodes")
		}
		for _, node := range nodes {
			addresses = append(addresses, node.Address)
		}
		return nil
	}); err != nil {
		return nil, errors.WithStack(err)
	}

	var nodes []db.NodeInfo
	var offlineThreshold time.Duration

	if err := l.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		nodes, err = tx.Nodes()
		if err != nil {
			return errors.WithStack(err)
		}
		offlineThreshold, err = tx.NodeOfflineThreshold()
		return errors.WithStack(err)
	}); err != nil {
		return nil, errors.WithStack(err)
	}

	if len(nodes) == 0 {
		return make([]ClusterMember, 0), nil
	}

	return l.status(nodes, addresses, offlineThreshold)
}

func (l *List) status(nodes []db.NodeInfo, addresses []string, offlineThreshold time.Duration) ([]ClusterMember, error) {
	now := l.clock.Now()
	result := make([]ClusterMember, len(nodes))
	nodeVersion := nodes[0].Version()

	for i, node := range nodes {
		result[i].ServerName = node.Name
		result[i].URL = node.Address
		result[i].Database = contains(node.Address, addresses)

		if node.IsOffline(l.clock, offlineThreshold) {
			result[i].Status = StatusOffline
			result[i].Message = fmt.Sprintf(
				"no heartbeat since %s", now.Sub(node.Heartbeat).Round(time.Millisecond),
			)
		} else {
			result[i].Status = StatusOnline
			result[i].Message = "fully operational"
		}

		n, err := version.CompareVersions(nodeVersion, node.Version())
		if err != nil {
			result[i].Status = StatusBroken
			result[i].Message = "inconsistent version"
		}

		if n == 1 {
			// This node's version is lower, which means the version that the
			// previous node in the loop has been upgraded.
			nodeVersion = node.Version()
		}
	}

	// Update the state of online nodes that have been upgraded and whose
	// schema is more recent than the rest of the nodes.
	for i, node := range nodes {
		if result[i].Status != StatusOnline {
			continue
		}
		n, err := version.CompareVersions(nodeVersion, node.Version())
		if err != nil {
			continue
		}
		if n == 2 {
			result[i].Status = StatusBlocked
			result[i].Message = "waiting for other nodes to be upgraded"
		}
	}

	return result, nil
}
