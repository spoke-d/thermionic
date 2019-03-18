package notifier

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/pkg/client"
)

// State is a gateway to the two main stateful components, the database
// and the operating system. It's typically used by model entities in order to
// perform changes.
type State interface {
	// Node returns the underlying Node
	Node() Node

	// Cluster returns the underlying Cluster
	Cluster() Cluster
}

// Node mediates access to the data stored in the node-local SQLite database.
type Node interface {
	db.NodeTransactioner
}

// Cluster mediates access to data stored in the cluster dqlite database.
type Cluster interface {
	db.ClusterTransactioner

	// EnterExclusive acquires a lock on the cluster db, so any successive call to
	// Transaction will block until ExitExclusive has been called.
	EnterExclusive() error

	// ExitExclusive runs the given transaction and then releases the lock acquired
	// with EnterExclusive.
	ExitExclusive(func(*db.ClusterTx) error) error

	// NodeID sets the the node NodeID associated with this cluster instance. It's used for
	// backward-compatibility of all db-related APIs that were written before
	// clustering and don't accept a node NodeID, so in those cases we automatically
	// use this value as implicit node NodeID.
	NodeID(int64)
}

// Policy can be used to tweak the behavior of NewNotifier in case of
// some nodes are down.
type Policy int

// Possible notification policies.
const (
	NotifyAll   Policy = iota // Requires that all nodes are up.
	NotifyAlive               // Only notifies nodes that are alive
)

// Notifier is a function that invokes the given function against each node in
// the cluster excluding the invoking one.
type Notifier struct {
	state            State
	cert             *cert.Info
	clock            clock.Clock
	nodeConfigSchema config.Schema
}

// New builds a Notifier that can be used to notify other peers using
// the given policy.
func New(state State, cert *cert.Info, nodeConfigSchema config.Schema, options ...Option) *Notifier {
	opts := newOptions()
	opts.state = state
	opts.certInfo = cert
	for _, option := range options {
		option(opts)
	}

	return &Notifier{
		state:            opts.state,
		cert:             opts.certInfo,
		clock:            opts.clock,
		nodeConfigSchema: nodeConfigSchema,
	}
}

// Run is a function that invokes the given function against each node in
// the cluster excluding the invoking one.
func (n *Notifier) Run(fn func(*client.Client) error, policy Policy) error {
	var config *node.Config
	if err := n.state.Node().Transaction(func(tx *db.NodeTx) error {
		var err error
		config, err = node.ConfigLoad(tx, n.nodeConfigSchema)
		return errors.WithStack(err)
	}); err != nil {
		return errors.WithStack(err)
	}

	address, err := config.HTTPSAddress()
	if err != nil {
		return errors.WithStack(err)
	}
	// Fast-track the case where we're not networked at all.
	if address == "" {
		return nil
	}

	var peers []string
	if err := n.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		offlineThreshold, err := tx.NodeOfflineThreshold()
		if err != nil {
			return errors.WithStack(err)
		}

		nodes, err := tx.Nodes()
		if err != nil {
			return errors.WithStack(err)
		}
		for _, node := range nodes {
			if node.Address == address || node.Address == "0.0.0.0" {
				continue
			}
			if node.IsOffline(n.clock, offlineThreshold) {
				switch policy {
				case NotifyAll:
					return errors.Errorf("peer node %q is down", node.Address)
				case NotifyAlive:
					continue
				}
			}
			peers = append(peers, node.Address)
		}
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}

	errs := make([]error, len(peers))
	var wg sync.WaitGroup
	wg.Add(len(peers))

	for i, address := range peers {
		go func(i int, address string) {
			defer wg.Done()

			rawClient, err := client.New(
				fmt.Sprintf("https://%s", address),
				client.WithTLSServerCert(string(n.cert.PublicKey())),
				client.WithTLSClientCert(string(n.cert.PublicKey())),
				client.WithTLSClientKey(string(n.cert.PrivateKey())),
				client.WithUserAgent("cluster-notifier"),
			)
			if err != nil {
				errs[i] = errors.Wrapf(err, "failed to connect to peer %q", address)
				return
			}

			// Pass something meaningful here
			if err := fn(rawClient); err != nil {
				errs[i] = errors.Wrapf(err, "failed to notify peer %q", address)
			}
		}(i, address)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			if policy == NotifyAlive {
				// log out something meaning full
				continue
			}
			return errors.WithStack(err)
		}
	}
	return nil
}
