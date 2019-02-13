package membership

import (
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/pkg/errors"
)

// EnabledOption to be passed to NewEnabled to customize the resulting
// instance.
type EnabledOption func(*enabledOptions)

type enabledOptions struct {
	state State
}

// WithStateForEnabled sets the state on the options
func WithStateForEnabled(state State) EnabledOption {
	return func(options *enabledOptions) {
		options.state = state
	}
}

// Create a options instance with default values.
func newEnabledOptions() *enabledOptions {
	return &enabledOptions{}
}

// Enabled is a convenience that returns true if clustering is enabled on this
// node.
type Enabled struct {
	state State
}

// NewEnabled creates a Enabled with sane defaults.
func NewEnabled(state State, options ...EnabledOption) *Enabled {
	opts := newEnabledOptions()
	opts.state = state
	for _, option := range options {
		option(opts)
	}

	return &Enabled{
		state: opts.state,
	}
}

// Run executes the nodes enabled and returns an error if something goes wrong
func (p *Enabled) Run() (bool, error) {
	var enabled bool
	err := p.state.Node().Transaction(func(tx *db.NodeTx) error {
		addresses, err := tx.RaftNodeAddresses()
		if err != nil {
			return errors.WithStack(err)
		}
		enabled = len(addresses) > 0
		return nil
	})
	return enabled, errors.WithStack(err)
}
