package db

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// ServiceNodes returns all service services part of the cluster.
func (c *ClusterTx) ServiceNodes() ([]ServiceNodeInfo, error) {
	return c.serviceNodes("")
}

// ServiceByName returns the service with the given name.
func (c *ClusterTx) ServiceByName(name string) (ServiceNodeInfo, error) {
	services, err := c.serviceNodes("name=?", name)
	if err != nil {
		return ServiceNodeInfo{}, err
	}
	switch len(services) {
	case 0:
		return ServiceNodeInfo{}, ErrNoSuchObject
	case 1:
		return services[0], nil
	default:
		return ServiceNodeInfo{}, errors.Errorf("more than one service node matches")
	}
}

// ServiceAdd adds a service to the current list of services that are part of
// the cluster. It returns the ID of the newly inserted row.
func (c *ClusterTx) ServiceAdd(
	name,
	address,
	daemonAddress, daemonNonce string,
) (int64, error) {
	columns := []string{
		"name",
		"address",
		"daemon_address",
		"daemon_nonce",
	}
	values := []interface{}{
		name,
		address,
		daemonAddress,
		daemonNonce,
	}
	return c.query.UpsertObject(c.tx, "services", columns, values)
}

// ServiceUpdate updates the name an address of a service.
func (c *ClusterTx) ServiceUpdate(
	id int64,
	name,
	address,
	daemonAddress, daemonNonce string,
) error {
	result, err := c.tx.Exec("UPDATE services SET name=?, address=?, daemon_address=?, daemon_nonce=? WHERE id=?", name, address, daemonAddress, daemonNonce, id)
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

// ServiceRemove removes the service with the given id.
func (c *ClusterTx) ServiceRemove(id int64) error {
	result, err := c.tx.Exec("DELETE FROM services WHERE id=?", id)
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

// ServiceNodeOfflineThreshold returns the amount of time that needs to elapse
// after which a series of unsuccessful heartbeat will make the service node be
// considered offline.
func (c *ClusterTx) ServiceNodeOfflineThreshold() (time.Duration, error) {
	threshold := time.Duration(DiscoveryDefaultOfflineThreshold) * time.Second
	values, err := c.query.SelectStrings(
		c.tx, "SELECT value FROM config WHERE key='discovery.offline_threshold'")
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

// ServiceNodes returns all service nodes part of the cluster.
func (c *ClusterTx) serviceNodes(where string, args ...interface{}) ([]ServiceNodeInfo, error) {
	var nodes []ServiceNodeInfo
	dest := func(i int) []interface{} {
		nodes = append(nodes, ServiceNodeInfo{})
		return []interface{}{
			&nodes[i].ID,
			&nodes[i].Name,
			&nodes[i].Address,
			&nodes[i].DaemonAddress,
			&nodes[i].DaemonNonce,
			&nodes[i].Heartbeat,
		}
	}
	stmt := `SELECT id, name, address, daemon_address, daemon_nonce, heartbeat FROM services `
	if where != "" {
		stmt += fmt.Sprintf("WHERE %s ", where)
	}
	stmt += "ORDER BY id"
	err := c.query.SelectObjects(c.tx, dest, stmt, args...)
	return nodes, errors.Wrap(err, "failed to fetch services")
}
