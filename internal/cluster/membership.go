package cluster

import (
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/state"
)

// Count is a convenience for checking the current number of nodes in the
// cluster.
func Count(state state.State) (int, error) {
	var count int
	err := state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		var err error
		count, err = tx.NodesCount()
		return err
	})
	return count, err
}

// Enabled is a convenience that returns true if clustering is enabled on this
// node.
func Enabled(node Node) (bool, error) {
	enabled := false
	err := node.Transaction(func(tx *db.NodeTx) error {
		addresses, err := tx.RaftNodeAddresses()
		if err != nil {
			return err
		}
		enabled = len(addresses) > 0
		return nil
	})
	return enabled, err
}
