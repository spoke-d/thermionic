// +build integration

package cluster_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/query"
	"github.com/spoke-d/thermionic/internal/fsys"
)

// If the node is not clustered, the schema updates works normally.
func TestEnsureSchema_NoClustered(t *testing.T) {
	fs := newFileSystem(t)
	db := newDB(t)

	addNode(t, db, "0.0.0.0", 2, 1)

	cluster := cluster.New(
		cluster.NewBasicAPIExtensions(1),
		cluster.NewSchema(fs),
		cluster.WithDatabase(db),
		cluster.WithFileSystem(fs),
	)

	ready, err := cluster.EnsureSchema("1.2.3.4:666", "/unused/db/dir")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := true, ready; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Exercise EnsureSchema failures when the cluster can't be upgraded right now.
func TestEnsureSchema_ClusterNotUpgradable(t *testing.T) {
	schema := 2
	apiExtensions := 1

	cases := []struct {
		title string
		setup func(*testing.T, database.DB)
		ready bool
		err   string
	}{
		{
			`a node's schema version is behind`,
			func(t *testing.T, db database.DB) {
				addNode(t, db, "1", schema, apiExtensions)
				addNode(t, db, "2", schema-1, apiExtensions)
			},
			false, // The schema was not updated
			"",    // No error is returned
		},
		{
			`a node's number of API extensions is behind`,
			func(t *testing.T, db database.DB) {
				addNode(t, db, "1", schema, apiExtensions)
				addNode(t, db, "2", schema, apiExtensions-1)
			},
			false, // The schema was not updated
			"",    // No error is returned
		},
		{
			`this node's schema is behind`,
			func(t *testing.T, db database.DB) {
				addNode(t, db, "1", schema, apiExtensions)
				addNode(t, db, "2", schema+1, apiExtensions)
			},
			false,
			"this node's version is behind, please upgrade",
		},
		{
			`this node's number of API extensions is behind`,
			func(t *testing.T, db database.DB) {
				addNode(t, db, "1", schema, apiExtensions)
				addNode(t, db, "2", schema, apiExtensions+1)
			},
			false,
			"this node's version is behind, please upgrade",
		},
		{
			`inconsistent schema version and API extensions number`,
			func(t *testing.T, db database.DB) {
				addNode(t, db, "1", schema, apiExtensions)
				addNode(t, db, "2", schema+1, apiExtensions-1)
			},
			false,
			"nodes have inconsistent versions",
		},
	}
	for _, c := range cases {
		t.Run(c.title, func(t *testing.T) {
			fs := newFileSystem(t)
			db := newDB(t)

			c.setup(t, db)

			cluster := cluster.New(
				cluster.NewBasicAPIExtensions(1),
				cluster.NewSchema(fs),
				cluster.WithDatabase(db),
				cluster.WithFileSystem(fs),
				cluster.WithSleeper(fastSleeper{}),
			)

			ready, err := cluster.EnsureSchema("1", "/unused/db/dir")
			if expected, actual := c.ready, ready; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
			if c.err == "" {
				if err != nil {
					t.Errorf("expected err to be nil: %v", err)
				}
			} else {
				if expected, actual := c.err, err.Error(); expected != actual {
					t.Errorf("expected: %v, actual: %v", expected, actual)
				}
			}
		})
	}
}

// Regardless of whether the schema could actually be upgraded or not, the
// version of this node gets updated.
func TestEnsureSchema_UpdateNodeVersion(t *testing.T) {
	schema := 2
	apiExtensions := 1

	cases := []struct {
		setup func(*testing.T, database.DB)
		ready bool
	}{
		{
			func(t *testing.T, db database.DB) {},
			true,
		},
		{
			func(t *testing.T, db database.DB) {
				// Add a node which is behind.
				addNode(t, db, "2", schema, apiExtensions-1)
			},
			true,
		},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%v", c.ready), func(t *testing.T) {
			fs := newFileSystem(t)
			db := newDB(t)

			cluster := cluster.New(
				cluster.NewBasicAPIExtensions(1),
				cluster.NewSchema(fs),
				cluster.WithDatabase(db),
				cluster.WithFileSystem(fs),
				cluster.WithSleeper(fastSleeper{}),
			)

			// Add ourselves with an older schema version and API
			// extensions number.
			addNode(t, db, "1", schema-1, apiExtensions-1)

			// Ensure the schema.
			ready, err := cluster.EnsureSchema("1", "/unused/db/dir")
			if err != nil {
				t.Errorf("expected err to be nil: %v", err)
			}
			if expected, actual := c.ready, ready; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}

			// Check that the nodes table was updated with our new
			// schema version and API extensions number.
			assertNode(t, db, "1", schema, apiExtensions)
		})
	}
}

// Return a new in-memory FileSystem
func newFileSystem(t *testing.T) fsys.FileSystem {
	return fsys.NewVirtualFileSystem()
}

// Create a new in-memory SQLite database with a fresh cluster schema.
func newDB(t *testing.T) database.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	createTableSchema := `
CREATE TABLE schema (
    id         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    version    INTEGER NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE (version)
);
`
	_, err = db.Exec(createTableSchema + cluster.FreshSchema())
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	return database.NewShimDB(db)
}

// Add a new node with the given address, schema version and number of api extensions.
func addNode(t *testing.T, db database.DB, address string, schema int, apiExtensions int) {
	err := query.Transaction(db, func(tx database.Tx) error {
		stmt := `
INSERT INTO nodes(name, address, schema, api_extensions) VALUES (?, ?, ?, ?)
`
		name := fmt.Sprintf("node at %s", address)
		_, err := tx.Exec(stmt, name, address, schema, apiExtensions)
		return err
	})
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
}

// Assert that the node with the given address has the given schema version and API
// extensions number.
func assertNode(t *testing.T, db database.DB, address string, schema int, apiExtensions int) {
	err := query.Transaction(db, func(tx database.Tx) error {
		where := "address=? AND schema=? AND api_extensions=?"
		n, err := query.Count(tx, "nodes", where, address, schema, apiExtensions)
		if expected, actual := 1, n; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		return err
	})
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
}

type fastSleeper struct{}

func (fastSleeper) Sleep(t time.Duration) {
	// We still want to sleep during the integration tests, but we don't want
	// to sleep for the entire duration. This just aims to improve the
	// responsive
	time.Sleep(t / 500)
}
