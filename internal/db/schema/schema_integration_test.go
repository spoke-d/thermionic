// +build integration

package schema_test

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/query"
	"github.com/spoke-d/thermionic/internal/db/schema"
	"github.com/spoke-d/thermionic/internal/fsys"
)

func TestNewFromMap(t *testing.T) {
	db := newDB(t)
	fs := newFileSystem(t)

	s := schema.New(fs, []schema.Update{
		updateCreateTable,
		updateInsertValue,
	})
	initial, err := s.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := 0, initial; expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

// If the database schema version is more recent than our update series, an
// error is returned.
func TestSchemaEnsure_VersionMostRecentThanExpected(t *testing.T) {
	schema, db := newSchemaAndDB(t)
	schema.Add(updateNoop)
	_, err := schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	schema, _ = newSchemaAndDB(t)
	_, err = schema.Ensure(db)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
	if expected, actual := "schema version '1' is more recent than expected '0'", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// If a "fresh" SQL statement for creating the schema from scratch is provided,
// but it fails to run, an error is returned.
func TestSchemaEnsure_FreshStatementError(t *testing.T) {
	schema, db := newSchemaAndDB(t)
	schema.Add(updateNoop)
	schema.Fresh("garbage")

	_, err := schema.Ensure(db)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
	if expected, actual := "cannot apply fresh schema: near \"garbage\": syntax error", err.Error(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// If the database schema contains "holes" in the applied versions, an error is
// returned.
func TestSchemaEnsure_MissingVersion(t *testing.T) {
	schema, db := newSchemaAndDB(t)
	schema.Add(updateNoop)
	_, err := schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	_, err = tx.Exec(`INSERT INTO schema (version, updated_at) VALUES (3, strftime("%s"))`)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	schema.Add(updateNoop)
	schema.Add(updateNoop)

	_, err = schema.Ensure(db)
	if err == nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := "missing updates: 1 -> 3", err.Error(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// If the schema has no update, the schema table gets created and has no version.
func TestSchemaEnsure_ZeroUpdates(t *testing.T) {
	schema, db := newSchemaAndDB(t)

	_, err := schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	versions, err := query.SelectIntegers(tx, "SELECT version FROM SCHEMA")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	var want []int
	if expected, actual := want, versions; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// If the schema has updates and no one was applied yet, all of them get
// applied.
func TestSchemaEnsure_ApplyAllUpdates(t *testing.T) {
	schema, db := newSchemaAndDB(t)
	schema.Add(updateCreateTable)
	schema.Add(updateInsertValue)

	initial, err := schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := 0, initial; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	// THe update version is recorded.
	versions, err := query.SelectIntegers(tx, "SELECT version FROM SCHEMA")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := []int{1, 2}, versions; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	// The two updates have been applied in order.
	ids, err := query.SelectIntegers(tx, "SELECT id FROM test")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := []int{1}, ids; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// If the schema schema has been created using a dump, the schema table will
// contain just one row with the update level associated with the dump. It's
// possible to apply further updates from there, and only these new ones will
// be inserted in the schema table.
func TestSchemaEnsure_ApplyAfterInitialDumpCreation(t *testing.T) {
	schema, db := newSchemaAndDB(t)
	schema.Add(updateCreateTable)
	schema.Add(updateAddColumn)
	_, err := schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	dump, err := schema.Dump(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	_, db = newSchemaAndDB(t)
	schema.Fresh(dump)
	_, err = schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	schema.Add(updateNoop)
	_, err = schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	// Only updates starting from the initial dump are recorded.
	versions, err := query.SelectIntegers(tx, "SELECT version FROM schema")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := []int{2, 3}, versions; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// If the schema has updates and part of them were already applied, only the
// missing ones are applied.
func TestSchemaEnsure_OnlyApplyMissing(t *testing.T) {
	schema, db := newSchemaAndDB(t)
	schema.Add(updateCreateTable)
	_, err := schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	schema.Add(updateInsertValue)
	initial, err := schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := 1, initial; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	// All update versions are recorded.
	versions, err := query.SelectIntegers(tx, "SELECT version FROM SCHEMA")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := []int{1, 2}, versions; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	// The two updates have been applied in order.
	ids, err := query.SelectIntegers(tx, "SELECT id FROM test")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := []int{1}, ids; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// If a update fails, an error is returned, and all previous changes are rolled
// back.
func TestSchemaEnsure_FailingUpdate(t *testing.T) {
	schema, db := newSchemaAndDB(t)
	schema.Add(updateCreateTable)
	schema.Add(updateBoom)
	_, err := schema.Ensure(db)
	if expected, actual := "failed to apply update 1: boom", err.Error(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	// Not update was applied.
	tables, err := query.SelectStrings(tx, "SELECT name FROM sqlite_master WHERE type = 'table'")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := "schema", tables; contains(actual, expected) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "test", tables; contains(actual, expected) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// If a hook fails, an error is returned, and all previous changes are rolled
// back.
func TestSchemaEnsure_FailingHook(t *testing.T) {
	schema, db := newSchemaAndDB(t)
	schema.Add(updateCreateTable)
	schema.Hook(func(int, database.Tx) error { return errors.Errorf("boom") })
	_, err := schema.Ensure(db)
	if expected, actual := "failed to execute hook (version 0): boom", err.Error(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	// Not update was applied.
	tables, err := query.SelectStrings(tx, "SELECT name FROM sqlite_master WHERE type = 'table'")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := "schema", tables; contains(actual, expected) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "test", tables; contains(actual, expected) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// If the schema check callback returns ErrGracefulAbort, the process is
// aborted, although every change performed so far gets still committed.
func TestSchemaEnsure_CheckGracefulAbort(t *testing.T) {
	check := func(current int, tx database.Tx) error {
		_, err := tx.Exec("CREATE TABLE test (n INTEGER)")
		if err != nil {
			t.Errorf("expected err to be nil: %v", err)
		}
		return schema.ErrGracefulAbort
	}

	schema, db := newSchemaAndDB(t)
	schema.Add(updateCreateTable)
	schema.Check(check)

	_, err := schema.Ensure(db)
	if expected, actual := "schema check gracefully aborted", err.Error(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	// The table created by the check function still got committed.
	// to insert the row was not.
	ids, err := query.SelectIntegers(tx, "SELECT n FROM test")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	var want []int
	if expected, actual := want, ids; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// The SQL text returns by Dump() can be used to create the schema from
// scratch, without applying each individual update.
func TestSchemaDump(t *testing.T) {
	schema, db := newSchemaAndDB(t)
	schema.Add(updateCreateTable)
	schema.Add(updateAddColumn)
	_, err := schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	dump, err := schema.Dump(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	_, db = newSchemaAndDB(t)
	schema.Fresh(dump)
	_, err = schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	// All update versions are in place.
	versions, err := query.SelectIntegers(tx, "SELECT version FROM schema")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := []int{2}, versions; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	// Both the table added by the first update and the extra column added
	// by the second update are there.
	_, err = tx.Exec("SELECT id, name FROM test")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
}

// If not all updates are applied, Dump() returns an error.
func TestSchemaDump_MissingUpdatees(t *testing.T) {
	schema, db := newSchemaAndDB(t)
	schema.Add(updateCreateTable)
	_, err := schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	schema.Add(updateAddColumn)

	_, err = schema.Dump(db)
	if expected, actual := "update level is 1, expected 2", err.Error(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestSchema_File(t *testing.T) {
	fs := newFileSystem(t)
	schema, db := schema.Empty(fs), newDB(t)

	// Add an update that would insert a value into a non-existing table.
	schema.Add(updateInsertValue)

	file, err := fs.Create("therm-db-schema")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	_, err = file.Write([]byte(`CREATE TABLE test (id INTEGER);
	INSERT INTO test VALUES (2);
	`))
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	file.Close()

	schema.File("therm-db-schema")

	_, err = schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	// The file does not exist anymore.
	if expected, actual := false, fs.Exists("therm-db-schema"); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	// The table was created, and the extra row inserted as well.
	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	ids, err := query.SelectIntegers(tx, "SELECT id FROM test ORDER BY id")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := []int{1, 2}, ids; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// A both a custom schema file path and a hook are set, the hook runs before
// the queries in the file are executed.
func TestSchema_File_Hook(t *testing.T) {
	fs := newFileSystem(t)
	schema, db := schema.Empty(fs), newDB(t)

	// Add an update that would insert a value into a non-existing table.
	schema.Add(updateInsertValue)

	// Add a custom schema update query file that inserts a value into a
	// non-existing table.
	file, err := fs.Create("therm-db-schema")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	_, err = file.Write([]byte(`INSERT INTO test VALUES (2);`))
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	file.Close()

	schema.File("therm-db-schema")

	// Add a hook that takes care of creating the test table, this shows
	// that it's run before anything else.
	schema.Hook(func(version int, tx database.Tx) error {
		if version == -1 {
			_, err := tx.Exec("CREATE TABLE test (id INTEGER)")
			return err
		}
		return nil
	})

	_, err = schema.Ensure(db)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	// The table was created, and the both rows inserted as well.
	tx, err := db.Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	ids, err := query.SelectIntegers(tx, "SELECT id FROM test ORDER BY id")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := []int{1, 2}, ids; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Return a new in-memory FileSystem
func newFileSystem(t *testing.T) fsys.FileSystem {
	return fsys.NewVirtualFileSystem()
}

// Return a new in-memory SQLite database.
func newDB(t *testing.T) database.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	return database.NewShimDB(db)
}

// Return both an empty schema and a test database.
func newSchemaAndDB(t *testing.T) (*schema.Schema, database.DB) {
	fs := newFileSystem(t)
	return schema.Empty(fs), newDB(t)
}

// An update that does nothing.
func updateNoop(database.Tx) error {
	return nil
}

// An update that creates a test table.
func updateCreateTable(tx database.Tx) error {
	_, err := tx.Exec("CREATE TABLE test (id INTEGER)")
	return err
}

// An update that inserts a value into the test table.
func updateInsertValue(tx database.Tx) error {
	_, err := tx.Exec("INSERT INTO test VALUES (1)")
	return err
}

// An update that adds a column to the test tabble.
func updateAddColumn(tx database.Tx) error {
	_, err := tx.Exec("ALTER TABLE test ADD COLUMN name TEXT")
	return err
}

// An update that unconditionally fails with an error.
func updateBoom(tx database.Tx) error {
	return errors.Errorf("boom")
}

func contains(a []string, b string) bool {
	for _, v := range a {
		if v == b {
			return true
		}
	}
	return false
}
