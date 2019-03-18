package query

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db/database"
)

// StmtCount provides a function for creating the sql template for querying.
var StmtCount = func(table, where string) string {
	stmt := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	if where != "" {
		stmt += fmt.Sprintf(" WHERE %s", where)
	}
	return stmt
}

// Count returns the number of rows in the given table.
func Count(tx database.Tx, table, where string, args ...interface{}) (int, error) {
	rows, err := tx.Query(StmtCount(table, where), args...)
	if err != nil {
		return -1, err
	}
	defer rows.Close()

	// For sanity, make sure we read one and only one row.
	if !rows.Next() {
		return -1, errors.Errorf("no rows returned")
	}

	var count int
	if err := rows.Scan(&count); err != nil {
		return -1, errors.Errorf("failed to scan count column")
	}
	if rows.Next() {
		return -1, errors.Errorf("more than one row returned")
	}
	if err = rows.Err(); err != nil {
		return -1, errors.WithStack(err)
	}

	return count, nil
}
