// +build integration

package db_test

import (
	"testing"

	"github.com/spoke-d/thermionic/internal/db/query"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
)

// Node database objects automatically initialize their schema as needed.
func TestNode_Schema(t *testing.T) {
	node, cleanup := libtesting.NewTestNode(t)
	defer cleanup()

	// The underlying node-level database has exactly one row in the schema
	// table.
	db := node.DB()
	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	n, err := query.Count(tx, "schema", "")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := 1, n; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	err = tx.Commit()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
}
