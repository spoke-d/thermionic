package cluster

import (
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/query"
)

// StmtUpdateNodeVersion defines the SQL template for updating a node version
const StmtUpdateNodeVersion = `
UPDATE nodes SET schema=?, api_extensions=? WHERE address=?
`

// StmtSelectNodesVersions defines the SQL template for selecting a schema and
// api_extensions from nodes.
const StmtSelectNodesVersions = `
SELECT schema, api_extensions FROM nodes
`

// Update the schema and api_extensions columns of the row in the nodes table
// that matches the given id.
//
// If not such row is found, an error is returned.
func updateNodeVersion(tx database.Tx, address string, count, apiExtensions int) error {
	result, err := tx.Exec(StmtUpdateNodeVersion, count, apiExtensions, address)
	if err != nil {
		return errors.WithStack(err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return errors.WithStack(err)
	}
	if n != 1 {
		return errors.Errorf("updated %d rows instead of 1", n)
	}
	return nil
}

// Return the number of rows in the nodes table that have their address column
// set to '0.0.0.0'.
func selectUnclusteredNodesCount(tx database.Tx) (int, error) {
	return query.Count(tx, "nodes", "address='0.0.0.0'")
}

// Return a slice of binary integer tuples. Each tuple contains the schema
// version and number of api extensions of a node in the cluster.
func selectNodesVersions(tx database.Tx) ([][2]int, error) {
	versions := make([][2]int, 0)

	dest := func(i int) []interface{} {
		versions = append(versions, [2]int{})
		return []interface{}{
			&versions[i][0],
			&versions[i][1],
		}
	}

	err := query.SelectObjects(tx, dest, StmtSelectNodesVersions)
	return versions, errors.WithStack(err)
}
