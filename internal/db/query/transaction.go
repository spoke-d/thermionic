package query

import (
	"database/sql"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/pkg/errors"
)

// Transaction executes the given function within a database transaction.
func Transaction(db database.DB, f func(database.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	if err := f(tx); err != nil {
		return rollback(tx, err)
	}

	err = tx.Commit()
	if err == sql.ErrTxDone {
		err = nil // Ignore duplicate commits/rollbacks
	}
	return errors.WithStack(err)
}

// Rollback a transaction after the given error occurred. If the rollback
// succeeds the given error is returned, otherwise a new error that wraps it
// gets generated and returned.
func rollback(tx database.Tx, reason error) error {
	err := tx.Rollback()
	if err != nil {
		return errors.Wrap(reason, err.Error())
	}

	return reason
}
