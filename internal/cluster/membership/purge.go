package membership

import (
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/pkg/errors"
)

// PurgeOption to be passed to NewPurge to customize the resulting
// instance.
type PurgeOption func(*purgeOptions)

type purgeOptions struct {
	state State
}

// WithStateForPurge sets the state on the options
func WithStateForPurge(state State) PurgeOption {
	return func(options *purgeOptions) {
		options.state = state
	}
}

// Create a options instance with default values.
func newPurgeOptions() *purgeOptions {
	return &purgeOptions{}
}

// Purge removes a node entirely from the cluster database.
type Purge struct {
	state State
}

// NewPurge creates a Purge with sane defaults.
func NewPurge(state State, options ...PurgeOption) *Purge {
	opts := newPurgeOptions()
	opts.state = state
	for _, option := range options {
		option(opts)
	}

	return &Purge{
		state: opts.state,
	}
}

// Run executes the purge and returns an error if something goes wrong
func (p *Purge) Run(name string) error {
	err := p.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		// Get the node (if it doesn't exists an error is returned).
		node, err := tx.NodeByName(name)
		if err != nil {
			return errors.Wrapf(err, "failed to get node %s", name)
		}

		if err := tx.NodeRemove(node.ID); err != nil {
			return errors.Wrapf(err, "failed to remove node %s", name)
		}
		return nil
	})
	return errors.WithStack(err)
}
