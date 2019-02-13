package cluster_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/fsys"
)

//go:generate mockgen -package mocks -destination mocks/db_mock.go github.com/spoke-d/thermionic/internal/db/database DB,Tx,Rows
//go:generate mockgen -package mocks -destination mocks/filesystem_mock.go github.com/spoke-d/thermionic/internal/fsys FileSystem
//go:generate mockgen -package mocks -destination mocks/node_mock.go github.com/spoke-d/thermionic/internal/cluster Node
//go:generate mockgen -package mocks -destination mocks/raft_provider_mock.go github.com/spoke-d/thermionic/internal/cluster RaftProvider
//go:generate mockgen -package mocks -destination mocks/raft_mock.go github.com/spoke-d/thermionic/internal/cluster RaftInstance
//go:generate mockgen -package mocks -destination mocks/server_provider_mock.go github.com/spoke-d/thermionic/internal/cluster ServerProvider
//go:generate mockgen -package mocks -destination mocks/server_mock.go github.com/spoke-d/thermionic/internal/cluster Server
//go:generate mockgen -package mocks -destination mocks/store_provider_mock.go github.com/spoke-d/thermionic/internal/cluster StoreProvider
//go:generate mockgen -package mocks -destination mocks/store_mock.go github.com/spoke-d/thermionic/internal/db/cluster ServerStore
//go:generate mockgen -package mocks -destination mocks/net_mock.go github.com/spoke-d/thermionic/internal/cluster Net
//go:generate mockgen -package mocks -destination mocks/sleeper_mock.go github.com/spoke-d/thermionic/internal/clock Sleeper
//go:generate mockgen -package mocks -destination mocks/stdlib_net_mock.go net Listener

// Return a new in-memory FileSystem
func NewFileSystem(t *testing.T) fsys.FileSystem {
	return fsys.NewLocalFileSystem(false)
}

// NewTestNode creates a new Node for testing purposes, along with a function
// that can be used to clean it up when done.
func NewTestNode(t *testing.T) (*db.Node, func()) {
	fs := NewFileSystem(t)
	dir, err := ioutil.TempDir("", "therm-db-test-node-")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	node := db.NewNode(fs)
	err = node.Open(dir, nil)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	cleanup := func() {
		err := node.Close()
		if err != nil {
			t.Errorf("expected err to be nil: %v", err)
		}
		err = os.RemoveAll(dir)
		if err != nil {
			t.Errorf("expected err to be nil: %v", err)
		}
	}

	return node, cleanup
}

// NewTestNodeTx returns a fresh NodeTx object, along with a function that can
// be called to cleanup state when done with it.
func NewTestNodeTx(t *testing.T) (*db.NodeTx, func()) {
	node, nodeCleanup := NewTestNode(t)

	tx, err := node.DB().Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	nodeTx := db.NewNodeTx(tx)

	cleanup := func() {
		err := tx.Commit()
		if err != nil {
			t.Errorf("expected err to be nil: %v", err)
		}
		nodeCleanup()
	}

	return nodeTx, cleanup
}
