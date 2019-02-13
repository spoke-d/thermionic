package upgraded

import (
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/notifier"
	"github.com/spoke-d/thermionic/internal/config"
)

// Option to be passed to Connect to customize the resulting instance.
type Option func(*options)

type options struct {
	state            State
	certInfo         *cert.Info
	nodeConfigSchema config.Schema
	notifierProvider NotifierProvider
}

// WithState sets the state on the options
func WithState(state State) Option {
	return func(options *options) {
		options.state = state
	}
}

// WithCert sets the certInfo on the options
func WithCert(certInfo *cert.Info) Option {
	return func(options *options) {
		options.certInfo = certInfo
	}
}

// WithNodeConfigSchema sets the node configuration schema on the options
func WithNodeConfigSchema(nodeConfigSchema config.Schema) Option {
	return func(options *options) {
		options.nodeConfigSchema = nodeConfigSchema
	}
}

// WithNotifierProvider sets the notifierProvider on the options
func WithNotifierProvider(notifierProvider NotifierProvider) Option {
	return func(options *options) {
		options.notifierProvider = notifierProvider
	}
}

// Create a options instance with default values.
func newOptions() *options {
	return &options{
		notifierProvider: notifierProvider{},
	}
}

type notifierProvider struct{}

func (notifierProvider) New(state State, cert *cert.Info, nodeConfigSchema config.Schema) Notifier {
	return notifier.New(notifyStateShim{
		state: state,
	}, cert, nodeConfigSchema)
}

type notifyStateShim struct {
	state State
}

func (s notifyStateShim) Node() notifier.Node {
	return s.state.Node()
}

func (s notifyStateShim) Cluster() notifier.Cluster {
	return s.state.Cluster()
}
