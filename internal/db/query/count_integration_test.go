// +build integration

package query_test

import (
	"database/sql"
	"strconv"
	"testing"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/query"
)

// Count returns the current number of rows.
func TestCount_Cases(t *testing.T) {
	cases := []struct {
		where string
		args  []interface{}
		count int
	}{
		{
			"id=?",
			[]interface{}{999},
			0,
		},
		{
			"id=?",
			[]interface{}{1},
			1,
		},
		{
			"",
			[]interface{}{},
			2,
		},
	}
	for _, c := range cases {
		t.Run(strconv.Itoa(c.count), func(t *testing.T) {
			tx := newTxForCount(t)
			count, err := query.Count(tx, "test", c.where, c.args...)
			if err != nil {
				t.Errorf("expected err to be nil: %v", err)
			}
			if expected, actual := c.count, count; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
		})
	}
}

// Return a new transaction against an in-memory SQLite database with a single
// test table and a few rows.
func newTxForCount(t *testing.T) database.Tx {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	_, err = db.Exec("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	_, err = db.Exec("INSERT INTO test VALUES (1), (2)")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	tx, err := database.ShimTx(db.Begin())
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	return tx
}
