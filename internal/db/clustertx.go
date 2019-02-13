package db

import (
	"github.com/spoke-d/thermionic/internal/db/database"
)

// ClusterTx models a single interaction with a cluster database.
//
// It wraps low-level database.Tx objects and offers a high-level API to fetch
// and update data.
type ClusterTx struct {
	tx     database.Tx // Handle to a transaction in the cluster dqlite database.
	query  Query
	nodeID int64 // Node ID of this instance.
}

// NewClusterTx creates a new transaction node with sane defaults
func NewClusterTx(tx database.Tx, nodeID int64) *ClusterTx {
	return NewClusterTxWithQuery(tx, nodeID, queryShim{})
}

// NewClusterTxWithQuery creates a new transaction node with sane defaults
func NewClusterTxWithQuery(tx database.Tx, nodeID int64, query Query) *ClusterTx {
	return &ClusterTx{
		tx:     tx,
		query:  query,
		nodeID: nodeID,
	}
}

// Config fetches all cluster config keys.
func (c *ClusterTx) Config() (map[string]string, error) {
	return c.query.SelectConfig(c.tx, "config", "")
}

// UpdateConfig updates the given cluster configuration keys in the
// config table. Config keys set to empty values will be deleted.
func (c *ClusterTx) UpdateConfig(values map[string]string) error {
	return c.query.UpdateConfig(c.tx, "config", values)
}

// NodeID sets the the node NodeID associated with this cluster transaction.
func (c *ClusterTx) NodeID(id int64) {
	c.nodeID = id
}
