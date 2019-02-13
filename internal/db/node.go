package db

import (
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/node"
	"github.com/spoke-d/thermionic/internal/db/schema"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/pkg/errors"
)

// NodeTransactioner represents a way to run transaction on the node
type NodeTransactioner interface {

	// Transaction creates a new NodeTx object and transactionally executes the
	// node-level database interactions invoked by the given function. If the
	// function returns no error, all database changes are committed to the
	// node-level database, otherwise they are rolled back.
	Transaction(f func(*NodeTx) error) error
}

// NodeOpener represents a way to open a node
type NodeOpener interface {

	// Open a new node.
	//
	// The function hook parameter is used by the daemon to mark all known
	// patch names as applied when a brand new database is created.
	Open(string, func(*Node) error) error
}

// ClusterDefaultOfflineThreshold is the default value for the
// cluster.offline_threshold configuration key, expressed in seconds.
const ClusterDefaultOfflineThreshold = 30

// DiscoveryDefaultOfflineThreshold is the default value for the
// discovery.offline_threshold configuration key, expressed in seconds.
const DiscoveryDefaultOfflineThreshold = 120

// QueryNode represents a local node in a cluster
type QueryNode interface {

	// Open the node-local database object.
	Open(string) error

	// EnsureSchema applies all relevant schema updates to the node-local
	// database.
	//
	// Return the initial schema version found before starting the update, along
	// with any error occurred.
	EnsureSchema(hookFn schema.Hook) (int, error)

	// DB return the current database source.
	DB() database.DB
}

type nodeTxBuilder func(database.Tx) *NodeTx

// Node mediates access to the data stored in the node-local SQLite database.
type Node struct {
	transaction Transaction // Handle the transactions to the database
	node        QueryNode
	dir         string // Reference to the directory where the database file lives.
	builder     nodeTxBuilder
}

// NewNode creates a new Node object.
func NewNode(fileSystem fsys.FileSystem) *Node {
	return &Node{
		node:        node.New(fileSystem),
		transaction: transactionShim{},
		builder:     NewNodeTx,
	}
}

// Open a new node.
//
// The fresh hook parameter is used by the daemon to mark all known patch names
// as applied when a brand new database is created.
func (n *Node) Open(dir string, fresh func(*Node) error) error {
	if err := n.node.Open(dir); err != nil {
		return errors.WithStack(err)
	}

	hook := func(version int, tx database.Tx) error {
		return nil
	}
	initial, err := n.node.EnsureSchema(hook)
	if err != nil {
		return errors.WithStack(err)
	}

	n.dir = dir

	if initial == 0 {
		if fresh != nil {
			if err := fresh(n); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

// Dir returns the directory of the underlying database file.
func (n *Node) Dir() string {
	return n.dir
}

// Transaction creates a new NodeTx object and transactionally executes the
// node-level database interactions invoked by the given function. If the
// function returns no error, all database changes are committed to the
// node-level database, otherwise they are rolled back.
func (n *Node) Transaction(f func(*NodeTx) error) error {
	return n.transaction.Transaction(n.node.DB(), func(tx database.Tx) error {
		return f(n.builder(tx))
	})
}

// Close the database facade.
func (n *Node) Close() error {
	return n.node.DB().Close()
}

// DB returns the low level database handle to the node-local SQLite
// database.
func (n *Node) DB() database.DB {
	return n.node.DB()
}
