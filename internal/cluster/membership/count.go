package membership

import (
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db"
)

// CountOption to be passed to NewCount to customize the resulting
// instance.
type CountOption func(*countOptions)

type countOptions struct {
	state State
}

// WithStateForCount sets the state on the options
func WithStateForCount(state State) CountOption {
	return func(options *countOptions) {
		options.state = state
	}
}

// Create a options instance with default values.
func newCountOptions() *countOptions {
	return &countOptions{}
}

// Count queries the number of nodes in the cluster database.
type Count struct {
	state State
}

// NewCount creates a Count with sane defaults.
func NewCount(state State, options ...CountOption) *Count {
	opts := newCountOptions()
	opts.state = state
	for _, option := range options {
		option(opts)
	}

	return &Count{
		state: opts.state,
	}
}

// Run executes the nodes count and returns an error if something goes wrong
func (p *Count) Run() (int, error) {
	var count int
	err := p.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		count, err = tx.NodesCount()
		return errors.WithStack(err)
	})
	return count, errors.WithStack(err)
}
