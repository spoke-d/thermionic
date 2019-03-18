package services

import (
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/db"
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
)

// ServiceMember represents a service node in the cluster.
type ServiceMember struct {
	ServerName    string
	ServerAddress string
	DaemonAddress string
	DaemonNonce   string
	Status        string
	Message       string
}

// List the service nodes of the cluster.
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
func (l *List) Run() ([]ServiceMember, error) {
	var nodes []db.ServiceNodeInfo
	var offlineThreshold time.Duration

	if err := l.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		nodes, err = tx.ServiceNodes()
		if err != nil {
			return errors.WithStack(err)
		}
		offlineThreshold, err = tx.ServiceNodeOfflineThreshold()
		return errors.WithStack(err)
	}); err != nil {
		return nil, errors.WithStack(err)
	}

	if len(nodes) == 0 {
		return make([]ServiceMember, 0), nil
	}

	return l.status(nodes, offlineThreshold)
}

func (l *List) status(nodes []db.ServiceNodeInfo, offlineThreshold time.Duration) ([]ServiceMember, error) {
	now := l.clock.Now()
	result := make([]ServiceMember, len(nodes))

	for i, node := range nodes {
		result[i].ServerName = node.Name
		result[i].ServerAddress = node.Address
		result[i].DaemonAddress = node.DaemonAddress
		result[i].DaemonNonce = node.DaemonNonce

		if node.IsOffline(l.clock, offlineThreshold) {
			result[i].Status = StatusOffline
			result[i].Message = fmt.Sprintf(
				"no heartbeat since %s", now.Sub(node.Heartbeat).Round(time.Millisecond),
			)
		} else {
			result[i].Status = StatusOnline
			result[i].Message = "fully operational"
		}
	}

	return result, nil
}
