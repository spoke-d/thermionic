package database

import (
	"database/sql"

	"github.com/pkg/errors"
)

// DB represents a way for a database to start transactions
type DB interface {
	// Begin starts a transaction. The default isolation level is dependent on
	// the driver.
	Begin() (Tx, error)

	Ping() error

	// Close closes the database, releasing any open resources.
	//
	// It is rare to Close a DB, as the DB handle is meant to be
	// long-lived and shared between many goroutines.
	Close() error
}

// Tx is an in-progress database transaction.
// A transaction must end with a call to Commit or Rollback.
type Tx interface {
	// Query executes a query that returns rows, typically a SELECT.
	Query(query string, args ...interface{}) (Rows, error)

	// Exec executes a query that doesn't return rows.
	// For example: an INSERT and UPDATE.
	Exec(query string, args ...interface{}) (sql.Result, error)

	// Commit commits the transaction.
	Commit() error

	// Rollback aborts the transaction.
	Rollback() error
}

// Rows is the result of a query. Its cursor starts before the first row
// of the result set. Use Next to advance through the rows:
type Rows interface {

	// Columns returns the column names.
	// Columns returns an error if the rows are closed, or if the rows
	// are from QueryRow and there was a deferred error.
	Columns() ([]string, error)

	// ColumnTypes returns column information such as column type, length,
	// and nullable. Some information may not be available from some drivers.
	ColumnTypes() ([]ColumnType, error)

	// Next prepares the next result row for reading with the Scan method. It
	// returns true on success, or false if there is no next result row or an error
	// happened while preparing it. Err should be consulted to distinguish between
	// the two cases.
	//
	// Every call to Scan, even the first one, must be preceded by a call to Next.
	Next() bool

	// Scan copies the columns in the current row into the values pointed
	// at by dest. The number of values in dest must be the same as the
	// number of columns in Rows.
	//
	// Scan converts columns read from the database into the following
	// common Go types and special types provided by the sql package:
	//
	//    *string
	//    *[]byte
	//    *int, *int8, *int16, *int32, *int64
	//    *uint, *uint8, *uint16, *uint32, *uint64
	//    *bool
	//    *float32, *float64
	//    *interface{}
	//    *RawBytes
	//    any type implementing Scanner (see Scanner docs)
	//
	// In the most simple case, if the type of the value from the source
	// column is an integer, bool or string type T and dest is of type *T,
	// Scan simply assigns the value through the pointer.
	//
	// Scan also converts between string and numeric types, as long as no
	// information would be lost. While Scan stringifies all numbers
	// scanned from numeric database columns into *string, scans into
	// numeric types are checked for overflow. For example, a float64 with
	// value 300 or a string with value "300" can scan into a uint16, but
	// not into a uint8, though float64(255) or "255" can scan into a
	// uint8. One exception is that scans of some float64 numbers to
	// strings may lose information when stringifying. In general, scan
	// floating point columns into *float64.
	//
	// If a dest argument has type *[]byte, Scan saves in that argument a
	// copy of the corresponding data. The copy is owned by the caller and
	// can be modified and held indefinitely. The copy can be avoided by
	// using an argument of type *RawBytes instead; see the documentation
	// for RawBytes for restrictions on its use.
	//
	// If an argument has type *interface{}, Scan copies the value
	// provided by the underlying driver without conversion. When scanning
	// from a source value of type []byte to *interface{}, a copy of the
	// slice is made and the caller owns the result.
	//
	// Source values of type time.Time may be scanned into values of type
	// *time.Time, *interface{}, *string, or *[]byte. When converting to
	// the latter two, time.Format3339Nano is used.
	//
	// Source values of type bool may be scanned into types *bool,
	// *interface{}, *string, *[]byte, or *RawBytes.
	//
	// For scanning into *bool, the source may be true, false, 1, 0, or
	// string inputs parseable by strconv.ParseBool.
	Scan(dest ...interface{}) error

	// Err returns the error, if any, that was encountered during iteration.
	// Err may be called after an explicit or implicit Close.
	Err() error

	// Close closes the Rows, preventing further enumeration. If Next is called
	// and returns false and there are no further result sets,
	// the Rows are closed automatically and it will suffice to check the
	// result of Err. Close is idempotent and does not affect the result of Err.
	Close() error
}

// ColumnType contains the name and type of a column.
type ColumnType interface {

	// DatabaseTypeName returns the database system name of the column type. If an empty
	// string is returned the driver type name is not supported.
	// Consult your driver documentation for a list of driver data types. Length specifiers
	// are not included.
	// Common type include "VARCHAR", "TEXT", "NVARCHAR", "DECIMAL", "BOOL", "INT", "BIGINT".
	DatabaseTypeName() string
}

// RawSQLSource returns the underlying database from the interface
// This is required when we have to deal with some libraries that explicitly
// require the *sql.DB type.
type RawSQLSource interface {
	Raw() *sql.DB
}

// DBAccessor allows direct access to the underlying source
type DBAccessor interface {

	// DB return the current database source.
	DB() DB
}

// RawSQLDatabase takes a DB and returns the underlying source to the database,
// without the sim.
func RawSQLDatabase(database DB) (*sql.DB, error) {
	if db, ok := database.(RawSQLSource); ok {
		return db.Raw(), nil
	}
	return nil, errors.Errorf("can not get the underlying raw sql database from %T", database)
}
