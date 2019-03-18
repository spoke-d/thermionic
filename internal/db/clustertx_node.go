package db

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/version"
)

// Nodes returns all nodes part of the cluster.
//
// If this instance is not clustered, a list with a single node whose
// address is 0.0.0.0 is returned.
func (c *ClusterTx) Nodes() ([]NodeInfo, error) {
	return c.nodes(false, "")
}

// NodeAddress returns the address of the node this method is invoked on.
func (c *ClusterTx) NodeAddress() (string, error) {
	stmt := "SELECT address FROM nodes WHERE id=?"
	addresses, err := c.query.SelectStrings(c.tx, stmt, c.nodeID)
	if err != nil {
		return "", errors.WithStack(err)
	}
	switch len(addresses) {
	case 0:
		return "", nil
	case 1:
		return addresses[0], nil
	default:
		return "", errors.Errorf("inconsistency: non-unique node ID")
	}
}

// NodeHeartbeat updates the heartbeat column of the node with the given address.
func (c *ClusterTx) NodeHeartbeat(address string, heartbeat time.Time) error {
	stmt := "UPDATE nodes SET heartbeat=? WHERE address=?"
	result, err := c.tx.Exec(stmt, heartbeat, address)
	if err != nil {
		return errors.WithStack(err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return errors.WithStack(err)
	}
	if n != 1 {
		return errors.Errorf("expected to update one row and not %d", n)
	}
	return nil
}

// NodeAdd adds a node to the current list of nodes that are part of the
// cluster. It returns the ID of the newly inserted row.
func (c *ClusterTx) NodeAdd(name, address string, schema, api int) (int64, error) {
	columns := []string{
		"name",
		"address",
		"schema",
		"api_extensions",
	}
	values := []interface{}{
		name,
		address,
		schema,
		api,
	}
	return c.query.UpsertObject(c.tx, "nodes", columns, values)
}

// NodePending toggles the pending flag for the node. A node is pending when
// it's been accepted in the cluster, but has not yet actually joined it.
func (c *ClusterTx) NodePending(id int64, pending bool) error {
	value := 0
	if pending {
		value = 1
	}
	result, err := c.tx.Exec("UPDATE nodes SET pending=? WHERE id=?", value, id)
	if err != nil {
		return errors.WithStack(err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return errors.WithStack(err)
	}
	if n != 1 {
		return errors.Errorf("query updated %d rows instead of 1", n)
	}
	return nil
}

// NodeUpdate updates the name an address of a node.
func (c *ClusterTx) NodeUpdate(id int64, name, address string) error {
	result, err := c.tx.Exec("UPDATE nodes SET name=?, address=? WHERE id=?", name, address, id)
	if err != nil {
		return errors.WithStack(err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return errors.WithStack(err)
	}
	if n != 1 {
		return errors.Errorf("query updated %d rows instead of 1", n)
	}
	return nil
}

// NodeRemove removes the node with the given id.
func (c *ClusterTx) NodeRemove(id int64) error {
	result, err := c.tx.Exec("DELETE FROM nodes WHERE id=?", id)
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

// NodeByName returns the node with the given name.
func (c *ClusterTx) NodeByName(name string) (NodeInfo, error) {
	nodes, err := c.nodes(false, "name=?", name)
	if err != nil {
		return NodeInfo{}, err
	}
	switch len(nodes) {
	case 0:
		return NodeInfo{}, ErrNoSuchObject
	case 1:
		return nodes[0], nil
	default:
		return NodeInfo{}, errors.Errorf("more than one node matches")
	}
}

// NodeByAddress returns the pending node with the given network address.
func (c *ClusterTx) NodeByAddress(address string) (NodeInfo, error) {
	nodes, err := c.nodes(false, "address=?", address)
	if err != nil {
		return NodeInfo{}, err
	}
	switch len(nodes) {
	case 0:
		return NodeInfo{}, ErrNoSuchObject
	case 1:
		return nodes[0], nil
	default:
		return NodeInfo{}, errors.Errorf("more than one node matches")
	}
}

// NodePendingByAddress returns the pending node with the given network address.
func (c *ClusterTx) NodePendingByAddress(address string) (NodeInfo, error) {
	nodes, err := c.nodes(true, "address=?", address)
	if err != nil {
		return NodeInfo{}, err
	}
	switch len(nodes) {
	case 0:
		return NodeInfo{}, ErrNoSuchObject
	case 1:
		return nodes[0], nil
	default:
		return NodeInfo{}, errors.Errorf("more than one node matches")
	}
}

// NodeName returns the name of the node this method is invoked on.
func (c *ClusterTx) NodeName() (string, error) {
	stmt := "SELECT name FROM nodes WHERE id=?"
	names, err := c.query.SelectStrings(c.tx, stmt, c.nodeID)
	if err != nil {
		return "", errors.WithStack(err)
	}
	switch len(names) {
	case 0:
		return "", nil
	case 1:
		return names[0], nil
	default:
		return "", errors.Errorf("inconsistency: non-unique node ID")
	}
}

// NodeRename changes the name of an existing node.
//
// Return an error if a node with the same name already exists.
func (c *ClusterTx) NodeRename(old, new string) error {
	count, err := c.query.Count(c.tx, "nodes", "name=?", new)
	if err != nil {
		return errors.Wrap(err, "failed to check existing nodes")
	}
	if count != 0 {
		return ErrAlreadyDefined
	}
	stmt := `UPDATE nodes SET name=? WHERE name=?`
	result, err := c.tx.Exec(stmt, new, old)
	if err != nil {
		return errors.Wrap(err, "failed to update node name")
	}
	n, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get rows count")
	}
	if n != 1 {
		return errors.Errorf("expected to update one row, not %d", n)
	}
	return nil
}

// NodesCount returns the number of nodes in the cluster.
//
// Since there's always at least one node row, even when not-clustered, the
// return value is greater than zero
func (c *ClusterTx) NodesCount() (int, error) {
	count, err := c.query.Count(c.tx, "nodes", "")
	return count, errors.Wrap(err, "failed to count existing nodes")
}

// NodeIsEmpty returns an empty string if the node with the given ID has
// anything associated with it. Otherwise, it returns a message say what's left.
func (c *ClusterTx) NodeIsEmpty(id int64) (string, error) {
	// Note: there is currently nothing at the moment to identify if a node is
	// empty, so we just return nothing.
	return "", nil
}

// NodeIsOutdated returns true if there's some cluster node having an API or
// schema version greater than the node this method is invoked on.
func (c *ClusterTx) NodeIsOutdated() (bool, error) {
	nodes, err := c.nodes(false, "")
	if err != nil {
		return false, errors.Wrap(err, "failed to fetch nodes")
	}

	// Figure our own version.
	ver := [2]int{}
	for _, node := range nodes {
		if node.ID == c.nodeID {
			ver = node.Version()
		}
	}
	if ver[0] == 0 || ver[1] == 0 {
		return false, errors.Errorf("inconsistency: local node not found")
	}

	// Check if any of the other nodes is greater than us.
	for _, node := range nodes {
		if node.ID == c.nodeID {
			continue
		}
		n, err := version.CompareVersions(node.Version(), ver)
		if err != nil {
			return false, errors.Wrapf(err, "failed to compare with version of node %s", node.Name)
		}

		if n == 1 {
			// The other node's version is greater than ours.
			return true, nil
		}
	}

	return false, nil
}

// NodeUpdateVersion updates the schema and API version of the node with the
// given id. This is used only in tests.
func (c *ClusterTx) NodeUpdateVersion(id int64, version [2]int) error {
	stmt := "UPDATE nodes SET schema=?, api_extensions=? WHERE id=?"

	result, err := c.tx.Exec(stmt, version[0], version[1], id)
	if err != nil {
		return errors.Wrap(err, "failed to update nodes table")
	}

	n, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get affected rows")
	}
	if n != 1 {
		return errors.Errorf("expected exactly one row to be updated")
	}
	return nil
}

// NodeOfflineThreshold returns the amount of time that needs to elapse after
// which a series of unsuccessful heartbeat will make the node be considered
// offline.
func (c *ClusterTx) NodeOfflineThreshold() (time.Duration, error) {
	threshold := time.Duration(ClusterDefaultOfflineThreshold) * time.Second
	values, err := c.query.SelectStrings(
		c.tx, "SELECT value FROM config WHERE key='cluster.offline_threshold'")
	if err != nil {
		return -1, errors.WithStack(err)
	}
	if len(values) > 0 {
		seconds, err := strconv.Atoi(values[0])
		if err != nil {
			return -1, errors.WithStack(err)
		}
		threshold = time.Duration(seconds) * time.Second
	}
	return threshold, nil
}

// Nodes returns all nodes part of the cluster.
func (c *ClusterTx) nodes(pending bool, where string, args ...interface{}) ([]NodeInfo, error) {
	var nodes []NodeInfo
	dest := func(i int) []interface{} {
		nodes = append(nodes, NodeInfo{})
		return []interface{}{
			&nodes[i].ID,
			&nodes[i].Name,
			&nodes[i].Address,
			&nodes[i].Description,
			&nodes[i].Schema,
			&nodes[i].APIExtensions,
			&nodes[i].Heartbeat,
		}
	}
	if pending {
		args = append([]interface{}{1}, args...)
	} else {
		args = append([]interface{}{0}, args...)
	}
	stmt := `SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? `
	if where != "" {
		stmt += fmt.Sprintf("AND %s ", where)
	}
	stmt += "ORDER BY id"
	err := c.query.SelectObjects(c.tx, dest, stmt, args...)
	return nodes, errors.Wrap(err, "failed to fetch nodes")
}
