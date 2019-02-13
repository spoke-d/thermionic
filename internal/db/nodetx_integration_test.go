// +build integration

package db_test

import (
	"reflect"
	"testing"

	"github.com/spoke-d/thermionic/internal/db"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
)

// Node-local configuration values are initially empty.
func TestTx_Config(t *testing.T) {
	tx, cleanup := libtesting.NewTestNodeTx(t)
	defer cleanup()

	values, err := tx.Config()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := map[string]string{}, values; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Node-local configuration values can be updated with UpdateConfig.
func TestTx_UpdateConfig(t *testing.T) {
	tx, cleanup := libtesting.NewTestNodeTx(t)
	defer cleanup()

	err := tx.UpdateConfig(map[string]string{
		"foo": "x",
		"bar": "y",
	})
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	values, err := tx.Config()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	want := map[string]string{
		"foo": "x",
		"bar": "y",
	}
	if expected, actual := want, values; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Keys that are associated with empty strings are deleted.
func TestTx_UpdateConfigUnsetKeys(t *testing.T) {
	tx, cleanup := libtesting.NewTestNodeTx(t)
	defer cleanup()

	err := tx.UpdateConfig(map[string]string{"foo": "x", "bar": "y"})
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	err = tx.UpdateConfig(map[string]string{"foo": "x", "bar": ""})
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	values, err := tx.Config()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	want := map[string]string{
		"foo": "x",
	}
	if expected, actual := want, values; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Fetch all raft nodes.
func TestRaftNodes(t *testing.T) {
	tx, cleanup := libtesting.NewTestNodeTx(t)
	defer cleanup()

	id1, err := tx.RaftNodeAdd("1.2.3.4:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	id2, err := tx.RaftNodeAdd("5.6.7.8:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	nodes, err := tx.RaftNodes()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	if expected, actual := id1, nodes[0].ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := id2, nodes[1].ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "1.2.3.4:666", nodes[0].Address; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "5.6.7.8:666", nodes[1].Address; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Fetch the addresses of all raft nodes.
func TestRaftNodeAddresses(t *testing.T) {
	tx, cleanup := libtesting.NewTestNodeTx(t)
	defer cleanup()

	_, err := tx.RaftNodeAdd("1.2.3.4:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	_, err = tx.RaftNodeAdd("5.6.7.8:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	addresses, err := tx.RaftNodeAddresses()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := []string{"1.2.3.4:666", "5.6.7.8:666"}, addresses; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Fetch the address of the raft node with the given ID.
func TestRaftNodeAddress(t *testing.T) {
	tx, cleanup := libtesting.NewTestNodeTx(t)
	defer cleanup()

	_, err := tx.RaftNodeAdd("1.2.3.4:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	id, err := tx.RaftNodeAdd("5.6.7.8:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	address, err := tx.RaftNodeAddress(id)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := "5.6.7.8:666", address; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Add the first raft node.
func TestRaftNodeFirst(t *testing.T) {
	tx, cleanup := libtesting.NewTestNodeTx(t)
	defer cleanup()

	err := tx.RaftNodeFirst("1.2.3.4:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	err = tx.RaftNodeDelete(1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	err = tx.RaftNodeFirst("5.6.7.8:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	address, err := tx.RaftNodeAddress(1)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := "5.6.7.8:666", address; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Add a new raft node.
func TestRaftNodeAdd(t *testing.T) {
	tx, cleanup := libtesting.NewTestNodeTx(t)
	defer cleanup()

	id, err := tx.RaftNodeAdd("1.2.3.4:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := int64(1), id; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Delete an existing raft node.
func TestRaftNodeDelete(t *testing.T) {
	tx, cleanup := libtesting.NewTestNodeTx(t)
	defer cleanup()

	id, err := tx.RaftNodeAdd("1.2.3.4:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	err = tx.RaftNodeDelete(id)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
}

// Delete a non-existing raft node returns an error.
func TestRaftNodeDelete_NonExisting(t *testing.T) {
	tx, cleanup := libtesting.NewTestNodeTx(t)
	defer cleanup()

	err := tx.RaftNodeDelete(1)
	if expected, actual := db.ErrNoSuchObject, err; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Replace all existing raft nodes.
func TestRaftNodesReplace(t *testing.T) {
	tx, cleanup := libtesting.NewTestNodeTx(t)
	defer cleanup()

	_, err := tx.RaftNodeAdd("1.2.3.4:666")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	nodes := []db.RaftNode{
		{ID: 2, Address: "2.2.2.2:666"},
		{ID: 3, Address: "3.3.3.3:666"},
	}
	err = tx.RaftNodesReplace(nodes)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	newNodes, err := tx.RaftNodes()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := nodes, newNodes; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}
