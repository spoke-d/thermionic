package database

import (
	"database/sql"

	"github.com/pkg/errors"
)

// NewShimDB creates a wrapper around the underlying database from the sql
// package. This is required so that we can mock all the way down the type
// structure.
// Additionally we can also insert better errors and mertics when we need to
// so.
func NewShimDB(db *sql.DB) DB {
	return &databaseShim{
		db: db,
	}
}

// The following will shim the database to enable better logging and metrics
// at the query sites.

// ShimDB takes a db and err and returns a database shim
// See NewShimDB
func ShimDB(db *sql.DB, err error) (DB, error) {
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &databaseShim{
		db: db,
	}, nil
}

// ShimTx takes a db and err and returns a Tx shim
func ShimTx(tx *sql.Tx, err error) (Tx, error) {
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &txShim{
		tx: tx,
	}, nil
}

func shimRows(rows *sql.Rows, err error) (Rows, error) {
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &rowsShim{
		rows: rows,
	}, nil
}

type databaseShim struct {
	db *sql.DB
}

func (w *databaseShim) Begin() (Tx, error) {
	return ShimTx(w.db.Begin())
}

func (w *databaseShim) Ping() error {
	return w.db.Ping()
}

func (w *databaseShim) Close() error {
	return w.db.Close()
}

func (w *databaseShim) Raw() *sql.DB {
	return w.db
}

type txShim struct {
	tx *sql.Tx
}

func (w *txShim) Query(query string, args ...interface{}) (Rows, error) {
	return shimRows(w.tx.Query(query, args...))
}

func (w *txShim) Exec(query string, args ...interface{}) (sql.Result, error) {
	return w.tx.Exec(query, args...)
}

func (w *txShim) Commit() error {
	return w.tx.Commit()
}

func (w *txShim) Rollback() error {
	return w.tx.Rollback()
}

type rowsShim struct {
	rows *sql.Rows
}

func (w *rowsShim) Columns() ([]string, error) {
	return w.rows.Columns()
}

func (w *rowsShim) ColumnTypes() ([]ColumnType, error) {
	types, err := w.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	res := make([]ColumnType, len(types))
	for k, v := range types {
		res[k] = v
	}
	return res, nil
}

func (w *rowsShim) Next() bool {
	return w.rows.Next()
}

func (w *rowsShim) Scan(dest ...interface{}) error {
	return w.rows.Scan(dest...)
}

func (w *rowsShim) Err() error {
	return w.rows.Err()
}

func (w *rowsShim) Close() error {
	return w.rows.Close()
}
