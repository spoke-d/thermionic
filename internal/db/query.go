package db

import (
	"github.com/spoke-d/thermionic/internal/db/database"
	q "github.com/spoke-d/thermionic/internal/db/query"
)

// ConfigQuery defines queries to the database for configuration queries
type ConfigQuery interface {
	// SelectConfig executes a query statement against a "config" table, which
	// must have 'key' and 'value' columns. By default this query returns all
	// keys, but additional WHERE filters can be specified.
	//
	// Returns a map of key names to their associated values.
	SelectConfig(database.Tx, string, string, ...interface{}) (map[string]string, error)

	// UpdateConfig updates the given keys in the given table. Config keys set to
	// empty values will be deleted.
	UpdateConfig(database.Tx, string, map[string]string) error
}

// ObjectsQuery defines queries to the database for generic object queries
type ObjectsQuery interface {
	// SelectObjects executes a statement which must yield rows with a specific
	// columns schema. It invokes the given Dest hook for each yielded row.
	SelectObjects(database.Tx, q.Dest, string, ...interface{}) error

	// UpsertObject inserts or replaces a new row with the given column values,
	// to the given table using columns order. For example:
	//
	// UpsertObject(tx, "cars", []string{"id", "brand"}, []interface{}{1, "ferrari"})
	//
	// The number of elements in 'columns' must match the one in 'values'.
	UpsertObject(database.Tx, string, []string, []interface{}) (int64, error)

	// DeleteObject removes the row identified by the given ID. The given table
	// must have a primary key column called 'id'.
	//
	// It returns a flag indicating if a matching row was actually found and
	// deleted or not.
	DeleteObject(database.Tx, string, int64) (bool, error)
}

// StringsQuery defines queries to the database for string queries
type StringsQuery interface {

	// SelectStrings executes a statement which must yield rows with a single
	// string column. It returns the list of column values.
	SelectStrings(database.Tx, string, ...interface{}) ([]string, error)
}

// CountQuery defines queries to the database for count queries
type CountQuery interface {

	// Count returns the number of rows in the given table.
	Count(database.Tx, string, string, ...interface{}) (int, error)
}

// Query defines different queries for accessing the database
type Query interface {
	ConfigQuery
	ObjectsQuery
	StringsQuery
	CountQuery
}

// Transaction defines a method for executing transactions over the
// database
type Transaction interface {
	// Transaction executes the given function within a database transaction.
	Transaction(database.DB, func(database.Tx) error) error
}

type queryShim struct{}

func (queryShim) SelectConfig(tx database.Tx, table string, where string, args ...interface{}) (map[string]string, error) {
	return q.SelectConfig(tx, table, where, args...)
}

func (queryShim) UpdateConfig(tx database.Tx, table string, values map[string]string) error {
	return q.UpdateConfig(tx, table, values)
}

func (queryShim) SelectObjects(tx database.Tx, dest q.Dest, stmt string, args ...interface{}) error {
	return q.SelectObjects(tx, dest, stmt, args...)
}

func (queryShim) UpsertObject(tx database.Tx, table string, columns []string, values []interface{}) (int64, error) {
	return q.UpsertObject(tx, table, columns, values)
}

func (queryShim) DeleteObject(tx database.Tx, table string, id int64) (bool, error) {
	return q.DeleteObject(tx, table, id)
}

func (queryShim) SelectStrings(tx database.Tx, stmt string, args ...interface{}) ([]string, error) {
	return q.SelectStrings(tx, stmt, args...)
}

func (queryShim) Count(tx database.Tx, table, where string, args ...interface{}) (int, error) {
	return q.Count(tx, table, where, args...)
}

type transactionShim struct{}

func (transactionShim) Transaction(db database.DB, f func(database.Tx) error) error {
	return q.Transaction(db, f)
}
