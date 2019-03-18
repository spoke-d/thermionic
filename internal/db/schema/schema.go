package schema

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/query"
	"github.com/spoke-d/thermionic/internal/fsys"
)

// Schema captures the schema of a database in terms of a series of ordered
// updates.
type Schema struct {
	fileSystem fsys.FileSystem
	updates    []Update // Ordered series of updates making up the schema
	hook       Hook     // Optional hook to execute whenever a update gets applied
	fresh      string   // Optional SQL statement used to create schema from scratch
	check      Check    // Optional callback invoked before doing any update
	path       string   // Optional path to a file containing extra queries to run
}

// Update applies a specific schema change to a database, and returns an error
// if anything goes wrong.
type Update func(database.Tx) error

// Hook is a callback that gets fired when a update gets applied.
type Hook func(int, database.Tx) error

// Check is a callback that gets fired all the times Schema.Ensure is invoked,
// before applying any update. It gets passed the version that the schema is
// currently at and a handle to the transaction. If it returns nil, the update
// proceeds normally, otherwise it's aborted. If ErrGracefulAbort is returned,
// the transaction will still be committed, giving chance to this function to
// perform state changes.
type Check func(int, database.Tx) error

// New creates a new schema Schema with the given updates.
func New(fileSystem fsys.FileSystem, updates []Update) *Schema {
	return &Schema{
		fileSystem: fileSystem,
		updates:    updates,
	}
}

// Empty creates a new schema with no updates.
func Empty(fileSystem fsys.FileSystem) *Schema {
	return New(fileSystem, make([]Update, 0))
}

// Add a new update to the schema. It will be appended at the end of the
// existing series.
func (s *Schema) Add(update Update) {
	s.updates = append(s.updates, update)
}

// Len returns the number of total updates in the schema.
func (s *Schema) Len() int {
	return len(s.updates)
}

// Hook instructs the schema to invoke the given function whenever a update is
// about to be applied. The function gets passed the update version number and
// the running transaction, and if it returns an error it will cause the schema
// transaction to be rolled back. Any previously installed hook will be
// replaced.
func (s *Schema) Hook(hook Hook) {
	s.hook = hook
}

// Check instructs the schema to invoke the given function whenever Ensure is
// invoked, before applying any due update. It can be used for aborting the
// operation.
func (s *Schema) Check(check Check) {
	s.check = check
}

// Fresh sets a statement that will be used to create the schema from scratch
// when bootstraping an empty database. It should be a "flattening" of the
// available updates, generated using the Dump() method. If not given, all
// patches will be applied in order.
func (s *Schema) Fresh(statement string) {
	s.fresh = statement
}

// File extra queries from a file. If the file is exists, all SQL queries in it
// will be executed transactionally at the very start of Ensure(), before
// anything else is done.
//
//If a schema hook was set with Hook(), it will be run before running the
//queries in the file and it will be passed a patch version equals to -1.
func (s *Schema) File(path string) {
	s.path = path
}

// Trim the schema updates to the given version (included). Updates with higher
// versions will be discarded. Any fresh schema dump previously set will be
// unset, since it's assumed to no longer be applicable. Return all updates
// that have been trimmed.
func (s *Schema) Trim(version int) []Update {
	trimmed := s.updates[version:]
	s.updates = s.updates[:version]
	s.fresh = ""
	return trimmed
}

// Ensure makes sure that the actual schema in the given database matches the
// one defined by our updates.
//
// All updates are applied transactionally. In case any error occurs the
// transaction will be rolled back and the database will remain unchanged.
//
// A update will be applied only if it hasn't been before (currently applied
// updates are tracked in the a 'schema' table, which gets automatically
// created).
//
// If no error occurs, the integer returned by this method is the
// initial version that the schema has been upgraded from.
func (s *Schema) Ensure(src database.DB) (int, error) {
	var (
		current int
		aborted bool
	)
	err := query.Transaction(src, func(tx database.Tx) error {
		if err := execFromFile(s.fileSystem, tx, s.path, s.hook); err != nil {
			return errors.Wrapf(err, "failed to execute queries from %q", s.path)
		}

		if err := ensureSchemaTableExists(tx); err != nil {
			return errors.WithStack(err)
		}
		var err error
		current, err = queryCurrentVersion(tx)
		if err != nil {
			return errors.WithStack(err)
		}

		if s.check != nil {
			if err := s.check(current, tx); err == ErrGracefulAbort {
				// Abort the update gracefully, committing what we've done so
				// far.
				aborted = true
				return nil
			} else if err != nil {
				return errors.WithStack(err)
			}
		}

		// When creating the schema from scratch, use the fresh dump if
		// available. Otherwise just apply all relevant updates.
		if current == 0 && s.fresh != "" {
			if _, err := tx.Exec(s.fresh); err != nil {
				return errors.Wrap(err, "cannot apply fresh schema")
			}
		} else {
			err = ensureUpdatesAreApplied(tx, current, s.updates, s.hook)
			return errors.WithStack(err)
		}
		return nil
	})
	if err != nil {
		return -1, errors.WithStack(err)
	}
	if aborted {
		return current, ErrGracefulAbort
	}
	return current, nil
}

// Dump returns a text of SQL commands that can be used to create this schema
// from scratch in one go, without going thorugh individual patches
// (essentially flattening them).
//
// It requires that all patches in this schema have been applied, otherwise an
// error will be returned.
func (s *Schema) Dump(src database.DB) (string, error) {
	var statements []string
	if err := query.Transaction(src, func(tx database.Tx) error {
		err := checkAllUpdatesAreApplied(tx, s.updates)
		if err != nil {
			return errors.WithStack(err)
		}
		statements, err = selectTablesSQL(tx)
		return errors.WithStack(err)
	}); err != nil {
		return "", errors.WithStack(err)
	}
	for i, statement := range statements {
		statements[i] = formatSQL(statement)
	}

	// Add a statement for inserting the current schema version row.
	statements = append(
		statements,
		fmt.Sprintf(StmtDump, len(s.updates)))
	return strings.Join(statements, ";\n"), nil
}

// Ensure that the schema table exists.
func ensureSchemaTableExists(tx database.Tx) error {
	exists, err := SchemaTableExists(tx)
	if err != nil {
		return errors.Wrap(err, "failed to check if schema table is there")
	}
	if !exists {
		if err := createSchemaTable(tx); err != nil {
			return errors.Wrap(err, "failed to create schema table")
		}
	}
	return nil
}

// Return the highest update version currently applied. Zero means that no
// updates have been applied yet.
func queryCurrentVersion(tx database.Tx) (int, error) {
	versions, err := selectSchemaVersions(tx)
	if err != nil {
		return -1, errors.Wrap(err, "failed to fetch update versions")
	}

	var current int
	if len(versions) > 0 {
		if err := checkSchemaVersionsHaveNoHoles(versions); err != nil {
			return -1, errors.WithStack(err)
		}
		current = versions[len(versions)-1] // Highest recorded version
	}
	return current, nil
}

// Check that the given list of update version numbers doesn't have "holes",
// that is each version equal the preceding version plus 1.
func checkSchemaVersionsHaveNoHoles(versions []int) error {
	// Sanity check that there are no "holes" in the recorded
	// versions.
	for i := range versions[:len(versions)-1] {
		if versions[i+1] != versions[i]+1 {
			return errors.Errorf("missing updates: %d -> %d", versions[i], versions[i+1])
		}
	}
	return nil
}

// Apply any pending update that was not yet applied.
func ensureUpdatesAreApplied(tx database.Tx, current int, updates []Update, hook Hook) error {
	if current > len(updates) {
		return errors.Errorf(
			"schema version '%d' is more recent than expected '%d'",
			current, len(updates))
	}

	// If there are no updates, there's nothing to do.
	if len(updates) == 0 {
		return nil
	}

	// Apply missing updates.
	for _, update := range updates[current:] {
		if hook != nil {
			if err := hook(current, tx); err != nil {
				return errors.Wrapf(err, "failed to execute hook (version %d)", current)
			}
		}

		if err := update(tx); err != nil {
			return errors.Wrapf(err, "failed to apply update %d", current)
		}
		current++
		if err := insertSchemaVersion(tx, current); err != nil {
			return errors.Errorf("failed to insert version %d", current)
		}
	}

	return nil
}

// Check that all the given updates are applied.
func checkAllUpdatesAreApplied(tx database.Tx, updates []Update) error {
	versions, err := selectSchemaVersions(tx)
	if err != nil {
		return errors.Errorf("failed to fetch update versions: %v", err)
	}
	if len(versions) == 0 {
		return errors.Errorf("expected schema table to contain at least one row")
	}
	if err := checkSchemaVersionsHaveNoHoles(versions); err != nil {
		return err
	}
	if current := versions[len(versions)-1]; current != len(updates) {
		return errors.Errorf("update level is %d, expected %d", current, len(updates))
	}
	return nil
}

// Format the given SQL statement in a human-readable way.
//
// In particular make sure that each column definition in a CREATE TABLE clause
// is in its own row, since SQLite dumps occasionally stuff more than one
// column in the same line.
func formatSQL(statement string) string {
	lines := strings.Split(statement, "\n")
	for i, line := range lines {
		if strings.Contains(line, "UNIQUE") {
			// Let UNIQUE(x, y) constraints alone.
			continue
		}
		lines[i] = strings.Replace(line, ", ", ",\n    ", -1)
	}
	return strings.Join(lines, "\n")
}
