package query

import (
	"fmt"
	"strings"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/pkg/errors"
)

// SelectStrings executes a statement which must yield rows with a single string
// column. It returns the list of column values.
func SelectStrings(tx database.Tx, query string, args ...interface{}) ([]string, error) {
	var values []string
	scan := func(rows database.Rows) error {
		var value string
		if err := rows.Scan(&value); err != nil {
			return errors.WithStack(err)
		}
		values = append(values, value)
		return nil
	}

	err := scanSingleColumn(tx, query, args, "TEXT", scan)
	return values, errors.WithStack(err)
}

// SelectIntegers executes a statement which must yield rows with a single integer
// column. It returns the list of column values.
func SelectIntegers(tx database.Tx, query string, args ...interface{}) ([]int, error) {
	var values []int
	scan := func(rows database.Rows) error {
		var value int
		if err := rows.Scan(&value); err != nil {
			return errors.WithStack(err)
		}
		values = append(values, value)
		return nil
	}

	err := scanSingleColumn(tx, query, args, "INTEGER", scan)
	return values, errors.WithStack(err)
}

// InsertStrings inserts a new row for each of the given strings, using the
// given insert statement template, which must define exactly one insertion
// column and one substitution placeholder for the values. For example:
// InsertStrings(tx, "INSERT INTO foo(name) VALUES %s", []string{"bar"}).
func InsertStrings(tx database.Tx, stmt string, values []string) error {
	n := len(values)
	if n == 0 {
		return nil
	}

	params := make([]string, n)
	args := make([]interface{}, n)
	for i, value := range values {
		params[i] = "(?)"
		args[i] = value
	}

	stmt = fmt.Sprintf(stmt, strings.Join(params, ", "))
	_, err := tx.Exec(stmt, args...)
	return errors.WithStack(err)
}

// Function to scan a single row.
type scanFunc func(database.Rows) error

// Execute the given query and ensure that it yields rows with a single column
// of the given database type. For every row yielded, execute the given
// scanner.
func scanSingleColumn(tx database.Tx, query string, args []interface{}, typeName string, scan scanFunc) error {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()

	if err := checkRowsHaveOneColumnOfSpecificType(rows, typeName); err != nil {
		return errors.WithStack(err)
	}

	for rows.Next() {
		if err := scan(rows); err != nil {
			return errors.WithStack(err)
		}
	}

	err = rows.Err()
	return errors.WithStack(err)
}

// Check that the given result set yields rows with a single column of a
// specific type.
func checkRowsHaveOneColumnOfSpecificType(rows database.Rows, typeName string) error {
	types, err := rows.ColumnTypes()
	if err != nil {
		return errors.WithStack(err)
	}
	if len(types) != 1 {
		return errors.Errorf("query yields %d columns, not 1", len(types))
	}
	actualTypeName := strings.ToUpper(types[0].DatabaseTypeName())
	// If the actualTypeName is empty, there is nothing we can check against,
	// so in that instance we should just return as valid.
	if actualTypeName == "" {
		return nil
	}
	if actualTypeName != typeName {
		return errors.Errorf("query yields %q column, not %q", actualTypeName, typeName)
	}
	return nil
}
