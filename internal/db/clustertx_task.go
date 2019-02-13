package db

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

// Task holds information about a single operation running on a node
// in the cluster.
type Task struct {
	ID          int64  // Stable database identifier
	UUID        string // User-visible identifier
	Query       string
	Schedule    int64
	Result      string
	Status      int
	NodeAddress string // Address of the node the operation is running on
}

// Tasks returns all tasks associated with this node.
func (c *ClusterTx) Tasks() ([]Task, error) {
	return c.tasks("node_id=?", c.nodeID)
}

// TaskNodes returns a list of nodes that have running tasks
func (c *ClusterTx) TaskNodes() ([]string, error) {
	stmt := "SELECT DISTINCT nodes.address FROM tasks JOIN nodes ON nodes.id = node_id"
	return c.query.SelectStrings(c.tx, stmt)
}

// TasksUUIDs returns the UUIDs of all tasks associated with this
// node.
func (c *ClusterTx) TasksUUIDs() ([]string, error) {
	stmt := "SELECT uuid FROM tasks WHERE node_id=?"
	return c.query.SelectStrings(c.tx, stmt, c.nodeID)
}

// TaskByUUID returns the operation with the given UUID.
func (c *ClusterTx) TaskByUUID(uuid string) (Task, error) {
	tasks, err := c.tasks("uuid=?", uuid)
	if err != nil {
		return Task{}, errors.WithStack(err)
	}
	switch len(tasks) {
	case 0:
		return Task{}, ErrNoSuchObject
	case 1:
		return tasks[0], nil
	default:
		return Task{}, errors.Errorf("more than one node matches")
	}
}

// TasksByScheduleRange returns a series of tasks within the time range
func (c *ClusterTx) TasksByScheduleRange(from, to time.Time, status int) ([]Task, error) {
	tasks, err := c.tasks("(schedule >= ? AND schedule < ?) AND status=?", from.Unix(), to.Unix(), status)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return tasks, nil
}

// TaskAdd adds a new tasks to the table.
func (c *ClusterTx) TaskAdd(uuid, query string, schedule int64, status int) (int64, error) {
	columns := []string{
		"uuid",
		"node_id",
		"query",
		"schedule",
		"result",
		"status",
	}
	values := []interface{}{
		uuid,
		c.nodeID,
		query,
		schedule,
		"",
		status,
	}
	return c.query.UpsertObject(c.tx, "tasks", columns, values)
}

// TaskRemove removes the operation with the given UUID.
func (c *ClusterTx) TaskRemove(uuid string) error {
	result, err := c.tx.Exec("DELETE FROM tasks WHERE uuid=?", uuid)
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

// TaskUpdateResult updates the result of the operation with the given UUID
func (c *ClusterTx) TaskUpdateResult(uuid, res string, status int) error {
	result, err := c.tx.Exec("UPDATE tasks SET result=?, status=? WHERE uuid=?", res, status, uuid)
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

// Tasks returns all tasks in the cluster, filtered by the given clause.
func (c *ClusterTx) tasks(where string, args ...interface{}) ([]Task, error) {
	var tasks []Task
	dest := func(i int) []interface{} {
		tasks = append(tasks, Task{})
		return []interface{}{
			&tasks[i].ID,
			&tasks[i].UUID,
			&tasks[i].NodeAddress,
			&tasks[i].Query,
			&tasks[i].Schedule,
			&tasks[i].Result,
			&tasks[i].Status,
		}
	}
	stmt := `SELECT tasks.id, uuid, nodes.address, query, schedule, result, status FROM tasks JOIN nodes ON nodes.id = node_id `
	if where != "" {
		stmt += fmt.Sprintf("WHERE %s ", where)
	}
	stmt += "ORDER BY tasks.id"
	err := c.query.SelectObjects(c.tx, dest, stmt, args...)
	return tasks, errors.Wrap(err, "failed to fetch tasks")
}
