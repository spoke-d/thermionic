// +build integration

package db_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/db"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
)

// Add a new raft node.
func TestNodeAdd(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	clock := clock.New()

	id, err := tx.NodeAdd("buzz", "1.2.3.4:666", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := int64(2), id; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	nodes, err := tx.Nodes()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := 2, len(nodes); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	node, err := tx.NodeByAddress("1.2.3.4:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := "buzz", node.Name; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "1.2.3.4:666", node.Address; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := [2]int{1, 1}, node.Version(); !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := false, node.IsOffline(clock, 20*time.Second); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	node, err = tx.NodeByName("buzz")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := "buzz", node.Name; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestNodesCount(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	count, err := tx.NodesCount()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	// There's always at least one node.
	if expected, actual := 1, count; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	_, err = tx.NodeAdd("buzz", "1.2.3.4:666", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	count, err = tx.NodesCount()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := 2, count; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestNodeIsOutdated_SingleNode(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	outdated, err := tx.NodeIsOutdated()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := false, outdated; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestNodeIsOutdated_AllNodesAtSameVersion(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	_, err := tx.NodeAdd("buzz", "1.2.3.4:666", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	outdated, err := tx.NodeIsOutdated()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := false, outdated; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestNodeIsOutdated_OneNodeWithHigherVersion(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	id, err := tx.NodeAdd("buzz", "1.2.3.4:666", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	version := [2]int{1 + 1, 1}
	err = tx.NodeUpdateVersion(id, version)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	outdated, err := tx.NodeIsOutdated()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := true, outdated; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestNodeIsOutdated_OneNodeWithLowerVersion(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	id, err := tx.NodeAdd("buzz", "1.2.3.4:666", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	version := [2]int{1, 1 - 1}
	err = tx.NodeUpdateVersion(id, version)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	outdated, err := tx.NodeIsOutdated()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := false, outdated; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestNodeName(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	name, err := tx.NodeName()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := "none", name; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Rename a node
func TestNodeRename(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	_, err := tx.NodeAdd("buzz", "1.2.3.4:666", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	err = tx.NodeRename("buzz", "rusp")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	node, err := tx.NodeByName("rusp")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := "rusp", node.Name; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	_, err = tx.NodeAdd("buzz", "5.6.7.8:666", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	err = tx.NodeRename("rusp", "buzz")
	if expected, actual := db.ErrAlreadyDefined, err; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Remove a new raft node.
func TestNodeRemove(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	_, err := tx.NodeAdd("buzz", "1.2.3.4:666", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	id, err := tx.NodeAdd("rusp", "5.6.7.8:666", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	err = tx.NodeRemove(id)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	_, err = tx.NodeByName("buzz")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	_, err = tx.NodeByName("rusp")
	if expected, actual := db.ErrNoSuchObject, err; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Mark a node has pending.
func TestNodePending(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	id, err := tx.NodeAdd("buzz", "1.2.3.4:666", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	// Add the pending flag
	err = tx.NodePending(id, true)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	// Pending nodes are skipped from regular listing
	_, err = tx.NodeByName("buzz")
	if expected, actual := db.ErrNoSuchObject, err; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	nodes, err := tx.Nodes()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := 1, len(nodes); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	// But the key be retrieved with NodePendingByAddress
	node, err := tx.NodePendingByAddress("1.2.3.4:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := id, node.ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	// Remove the pending flag
	err = tx.NodePending(id, false)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	node, err = tx.NodeByName("buzz")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := id, node.ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Update the heartbeat of a node.
func TestNodeHeartbeat(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	_, err := tx.NodeAdd("buzz", "1.2.3.4:666", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	err = tx.NodeHeartbeat("1.2.3.4:666", time.Now().Add(-time.Minute))
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	nodes, err := tx.Nodes()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := 2, len(nodes); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	clock := clock.New()
	node := nodes[1]
	if expected, actual := true, node.IsOffline(clock, 20*time.Second); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Add, get and remove an operation.
func TestOperation(t *testing.T) {
	tx, cleanup := libtesting.NewTestClusterTx(t, 1, 1)
	defer cleanup()

	id, err := tx.OperationAdd("abcd", db.OperationType("lock"))
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := int64(1), id; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	operations, err := tx.Operations()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := 1, len(operations); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "abcd", operations[0].UUID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	operation, err := tx.OperationByUUID("abcd")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := id, operation.ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := db.OperationType("lock"), operation.Type; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	uuids, err := tx.OperationsUUIDs()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := []string{"abcd"}, uuids; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	err = tx.OperationRemove("abcd")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	_, err = tx.OperationByUUID("abcd")
	if expected, actual := db.ErrNoSuchObject, err; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}
