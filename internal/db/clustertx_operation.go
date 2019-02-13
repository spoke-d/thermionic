package db

import (
	"fmt"

	"github.com/pkg/errors"
)

// OperationType is a identifier code identifying the type of an Operation.
type OperationType string

// Operation holds information about a single operation running on a node
// in the cluster.
type Operation struct {
	ID          int64         // Stable database identifier
	UUID        string        // User-visible identifier
	NodeAddress string        // Address of the node the operation is running on
	Type        OperationType // Type of the operation
}

// Operations returns all operations associated with this node.
func (c *ClusterTx) Operations() ([]Operation, error) {
	return c.operations("node_id=?", c.nodeID)
}

// OperationNodes returns a list of nodes that have running operations
func (c *ClusterTx) OperationNodes() ([]string, error) {
	stmt := "SELECT DISTINCT nodes.address FROM operations JOIN nodes ON nodes.id = node_id"
	return c.query.SelectStrings(c.tx, stmt)
}

// OperationsUUIDs returns the UUIDs of all operations associated with this
// node.
func (c *ClusterTx) OperationsUUIDs() ([]string, error) {
	stmt := "SELECT uuid FROM operations WHERE node_id=?"
	return c.query.SelectStrings(c.tx, stmt, c.nodeID)
}

// OperationByUUID returns the operation with the given UUID.
func (c *ClusterTx) OperationByUUID(uuid string) (Operation, error) {
	operations, err := c.operations("uuid=?", uuid)
	if err != nil {
		return Operation{}, errors.WithStack(err)
	}
	switch len(operations) {
	case 0:
		return Operation{}, ErrNoSuchObject
	case 1:
		return operations[0], nil
	default:
		return Operation{}, errors.Errorf("more than one node matches")
	}
}

// OperationAdd adds a new operations to the table.
func (c *ClusterTx) OperationAdd(uuid string, opType OperationType) (int64, error) {
	columns := []string{
		"uuid",
		"node_id",
		"type",
	}
	values := []interface{}{
		uuid,
		c.nodeID,
		opType,
	}
	return c.query.UpsertObject(c.tx, "operations", columns, values)
}

// OperationRemove removes the operation with the given UUID.
func (c *ClusterTx) OperationRemove(uuid string) error {
	result, err := c.tx.Exec("DELETE FROM operations WHERE uuid=?", uuid)
	if err != nil {
		return errors.WithStack(err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return errors.WithStack(err)
	}
	if n != 1 {
		return errors.Errorf("query deleted %d rows instead of 1", n)
	}
	return nil
}

// Operations returns all operations in the cluster, filtered by the given clause.
func (c *ClusterTx) operations(where string, args ...interface{}) ([]Operation, error) {
	var operations []Operation
	dest := func(i int) []interface{} {
		operations = append(operations, Operation{})
		return []interface{}{
			&operations[i].ID,
			&operations[i].UUID,
			&operations[i].NodeAddress,
			&operations[i].Type,
		}
	}
	stmt := `SELECT operations.id, uuid, nodes.address, type FROM operations JOIN nodes ON nodes.id = node_id `
	if where != "" {
		stmt += fmt.Sprintf("WHERE %s ", where)
	}
	stmt += "ORDER BY operations.id"
	err := c.query.SelectObjects(c.tx, dest, stmt, args...)
	return operations, errors.Wrap(err, "failed to fetch operations")
}
