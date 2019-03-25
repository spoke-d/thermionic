package schema

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/query"
	"github.com/spoke-d/thermionic/internal/fsys"
)

// StmtSchemaTableExists represents a query for checking if the schema table
// exists.
const StmtSchemaTableExists = `
SELECT COUNT(name) FROM sqlite_master WHERE type = 'table' AND name = 'schema'
`

// StmtCreateTable represents a query for creating a schema table.
const StmtCreateTable = `
CREATE TABLE schema (
    id         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    version    INTEGER NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE (version)
)
`

// StmtSelectSchemaVersions represents a query to get the version from the
// schema table
const StmtSelectSchemaVersions = `
SELECT version FROM schema ORDER BY version
`

// StmtSelectTableSQL represents a query to get the sql from a table.
const StmtSelectTableSQL = `
SELECT sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name != 'schema' ORDER BY name
`

// StmtInsertSchemaVersion represents an insert query for inserting versions
// into the schema.
const StmtInsertSchemaVersion = `
INSERT INTO schema (version, updated_at) VALUES (?, strftime("%s"))
`

// StmtDump represents a query to insert a query when performing a dump query.
const StmtDump = `
INSERT INTO schema (version, updated_at) VALUES (%d, strftime("%%s"))
`

// SchemaTableExists return whether the schema table is present in the database.
func SchemaTableExists(tx database.Tx) (bool, error) {
	rows, err := tx.Query(StmtSchemaTableExists)
	if err != nil {
		return false, errors.WithStack(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return false, errors.Errorf("schema table query returned no rows")
	}

	var count int
	if err := rows.Scan(&count); err != nil {
		return false, errors.WithStack(err)
	}
	return count == 1, nil
}

// Create the schema table.
func createSchemaTable(tx database.Tx) error {
	_, err := tx.Exec(StmtCreateTable)
	return errors.WithStack(err)
}

// Return all versions in the schema table, in increasing order.
func selectSchemaVersions(tx database.Tx) ([]int, error) {
	return query.SelectIntegers(tx, StmtSelectSchemaVersions)
}

// Return a list of SQL statements that can be used to create all tables in the
// database.
func selectTablesSQL(tx database.Tx) ([]string, error) {
	return query.SelectStrings(tx, StmtSelectTableSQL)
}

// Insert a new version into the schema table.
func insertSchemaVersion(tx database.Tx, new int) error {
	_, err := tx.Exec(StmtInsertSchemaVersion, new)
	return errors.WithStack(err)
}

// Read the given file (if it exists) and executes all queries it contains.
func execFromFile(fileSystem fsys.FileSystem, tx database.Tx, path string, hook Hook) error {
	if !fileSystem.Exists(path) {
		return nil
	}

	file, err := fileSystem.Open(path)
	if err != nil {
		return errors.Wrap(err, "failed to open file")
	}
	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return errors.Wrap(err, "failed to read file")
	}

	if hook != nil {
		err := hook(-1, tx)
		if err != nil {
			return errors.Wrap(err, "failed to execute hook")
		}
	}

	if _, err := tx.Exec(string(bytes)); err != nil {
		return err
	}

	err = fileSystem.Remove(path)
	return errors.Wrap(err, "failed to remove file")
}
