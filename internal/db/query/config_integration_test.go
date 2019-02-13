// +build integration

package query_test

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/query"
)

func TestSelectConfig_Selects(t *testing.T) {
	tx := newTxForConfig(t)
	values, err := query.SelectConfig(tx, "test", "")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	want := map[string]string{
		"foo": "x",
		"bar": "zz",
	}
	if expected, actual := want, values; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestSelectConfig_WithFilters(t *testing.T) {
	tx := newTxForConfig(t)
	values, err := query.SelectConfig(tx, "test", "key=?", "bar")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	want := map[string]string{
		"bar": "zz",
	}
	if expected, actual := want, values; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// New keys are added to the table.
func TestUpdateConfig_NewKeys(t *testing.T) {
	tx := newTxForConfig(t)

	values := map[string]string{"foo": "y"}
	err := query.UpdateConfig(tx, "test", values)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	values, err = query.SelectConfig(tx, "test", "")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	want := map[string]string{
		"foo": "y",
		"bar": "zz",
	}
	if expected, actual := want, values; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Unset keys are deleted from the table.
func TestDeleteConfig_Delete(t *testing.T) {
	tx := newTxForConfig(t)
	values := map[string]string{"foo": ""}

	err := query.UpdateConfig(tx, "test", values)

	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	values, err = query.SelectConfig(tx, "test", "")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	want := map[string]string{
		"bar": "zz",
	}
	if expected, actual := want, values; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Return a new transaction against an in-memory SQLite database with a single
// test table populated with a few rows.
func newTxForConfig(t *testing.T) database.Tx {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	_, err = db.Exec("CREATE TABLE test (key TEXT NOT NULL, value TEXT)")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES ('foo', 'x'), ('bar', 'zz')")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	tx, err := database.ShimTx(db.Begin())
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	return tx
}
