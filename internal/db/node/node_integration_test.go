// +build integration

package node_test

import (
	"io/ioutil"
	"testing"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/node"
	"github.com/spoke-d/thermionic/internal/fsys"
)

// When the node-local database is created from scratch, the value for the
// initial patch is 0.
func TestEnsureSchema_CreatedEmptyDB(t *testing.T) {
	fs := newFileSystem(t)

	path, err := ioutil.TempDir("", "therm-db-node-test-")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	defer fs.RemoveAll(path)

	node := node.New(fs)
	err = node.Open(path)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	defer node.DB().Close()

	hookHasRun := false
	hook := func(int, database.Tx) error {
		hookHasRun = true
		return nil
	}
	initial, err := node.EnsureSchema(hook)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := 0, initial; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := false, hookHasRun; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Return a new in-memory FileSystem
func newFileSystem(t *testing.T) fsys.FileSystem {
	// local file system is used because the sqlite db doesn't know about any
	// other filesystem
	return fsys.NewLocalFileSystem(false)
}
