package config

import "github.com/spoke-d/thermionic/internal/config"

// Option to be passed to New to customize the resulting
// instance.
type Option func(*options)

type options struct {
	clusterTx Tx
	configMap config.Map
}

// WithNodeTx sets the clusterTx on the options
func WithNodeTx(clusterTx Tx) Option {
	return func(options *options) {
		options.clusterTx = clusterTx
	}
}

// WithMap sets the configMap on the options
func WithMap(configMap config.Map) Option {
	return func(options *options) {
		options.configMap = configMap
	}
}

// Create a options instance with default values.
func newOptions() *options {
	return &options{}
}
