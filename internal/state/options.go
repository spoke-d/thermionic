package state

import (
	"github.com/spoke-d/thermionic/internal/sys"
)

// Option to be passed to New to customize the resulting
// instance.
type Option func(*options)

type options struct {
	node    Node
	cluster Cluster
	os      OS
}

// WithNode sets the node on the options
func WithNode(node Node) Option {
	return func(options *options) {
		options.node = node
	}
}

// WithCluster sets the cluster on the options
func WithCluster(cluster Cluster) Option {
	return func(options *options) {
		options.cluster = cluster
	}
}

// WithOS sets the os on the options
func WithOS(os OS) Option {
	return func(options *options) {
		options.os = os
	}
}

// Create a options instance with default values.
func newOptions() *options {
	return &options{
		os: sys.DefaultOS(),
	}
}
