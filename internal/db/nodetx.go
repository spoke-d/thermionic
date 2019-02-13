package db

import (
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/pkg/errors"
)

// RaftNode holds information about a single node in the dqlite raft cluster.
type RaftNode struct {
	ID      int64  // Stable node identifier
	Address string // Network address of the node
}

// NodeTx models a single interaction with a node-local database.
//
// It wraps low-level db.Tx objects and offers a high-level API to fetch and
// update data.
type NodeTx struct {
	tx    database.Tx // Handle to a transaction in the node-level SQLite database.
	query Query
}

// NewNodeTx creates a new transaction node with sane defaults
func NewNodeTx(tx database.Tx) *NodeTx {
	return NewNodeTxWithQuery(tx, queryShim{})
}

// NewNodeTxWithQuery creates a new transaction node with sane defaults
func NewNodeTxWithQuery(tx database.Tx, query Query) *NodeTx {
	return &NodeTx{
		tx:    tx,
		query: query,
	}
}

// Config fetches all node-level config keys.
func (n *NodeTx) Config() (map[string]string, error) {
	return n.query.SelectConfig(n.tx, "config", "")
}

// UpdateConfig updates the given node-level configuration keys in the
// config table. Config keys set to empty values will be deleted.
func (n *NodeTx) UpdateConfig(values map[string]string) error {
	return n.query.UpdateConfig(n.tx, "config", values)
}

// RaftNodes returns information about all nodes that are members of the
// dqlite Raft cluster (possibly including the local node). If this
// instance is not running in clustered mode, an empty list is returned.
func (n *NodeTx) RaftNodes() ([]RaftNode, error) {
	var nodes []RaftNode
	dest := func(i int) []interface{} {
		nodes = append(nodes, RaftNode{})
		return []interface{}{
			&nodes[i].ID,
			&nodes[i].Address,
		}
	}
	err := n.query.SelectObjects(n.tx, dest, "SELECT id, address FROM raft_nodes ORDER BY id")
	return nodes, errors.Wrap(err, "failed to fetch raft nodes")
}

// RaftNodeAddresses returns the addresses of all nodes that are members of
// the dqlite Raft cluster (possibly including the local node). If this
// instance is not running in clustered mode, an empty list is returned.
func (n *NodeTx) RaftNodeAddresses() ([]string, error) {
	return n.query.SelectStrings(n.tx, "SELECT address FROM raft_nodes ORDER BY id")
}

// RaftNodeAddress returns the address of the raft node with the given ID,
// if any matching row exists.
func (n *NodeTx) RaftNodeAddress(id int64) (string, error) {
	stmt := "SELECT address FROM raft_nodes WHERE id=?"
	addresses, err := n.query.SelectStrings(n.tx, stmt, id)
	if err != nil {
		return "", errors.WithStack(err)
	}
	switch len(addresses) {
	case 0:
		return "", ErrNoSuchObject
	case 1:
		return addresses[0], nil
	default:
		// This should never happen since we have a UNIQUE constraint
		// on the raft_nodes.id column.
		return "", errors.Errorf("more than one match found")
	}
}

// RaftNodeFirst adds a the first node if the cluster. It ensures that the
// database ID is 1, to match the server ID of first raft log entry.
//
// This method is supposed to be called when there are no rows in raft_nodes,
// and it will replace whatever existing row has ID 1.
func (n *NodeTx) RaftNodeFirst(address string) error {
	columns := []string{
		"id",
		"address",
	}
	values := []interface{}{
		int64(1),
		address,
	}
	id, err := n.query.UpsertObject(n.tx, "raft_nodes", columns, values)
	if err != nil {
		return errors.WithStack(err)
	}
	if id != 1 {
		return errors.Errorf("could not set raft node ID to 1")
	}
	return nil
}

// RaftNodeAdd adds a node to the current list of nodes that are part of the
// dqlite Raft cluster. It returns the ID of the newly inserted row.
func (n *NodeTx) RaftNodeAdd(address string) (int64, error) {
	columns := []string{
		"address",
	}
	values := []interface{}{
		address,
	}
	return n.query.UpsertObject(n.tx, "raft_nodes", columns, values)
}

// RaftNodeDelete removes a node from the current list of nodes that are
// part of the dqlite Raft cluster.
func (n *NodeTx) RaftNodeDelete(id int64) error {
	deleted, err := n.query.DeleteObject(n.tx, "raft_nodes", id)
	if err != nil {
		return errors.WithStack(err)
	}
	if !deleted {
		return ErrNoSuchObject
	}
	return nil
}

// RaftNodesReplace replaces the current list of raft nodes.
func (n *NodeTx) RaftNodesReplace(nodes []RaftNode) error {
	if _, err := n.tx.Exec("DELETE FROM raft_nodes"); err != nil {
		return errors.WithStack(err)
	}

	columns := []string{
		"id",
		"address",
	}
	for _, node := range nodes {
		values := []interface{}{
			node.ID,
			node.Address,
		}
		if _, err := n.query.UpsertObject(n.tx, "raft_nodes", columns, values); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
