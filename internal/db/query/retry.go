package query

import (
	"strings"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/retrier"
)

// Retry wraps a function that interacts with the database, and retries it in
// case a transient error is hit.
//
// This should by typically used to wrap transactions.
func Retry(sleeper clock.Sleeper, f func() error) error {
	retry := retrier.New(sleeper, 10, 250*time.Millisecond)
	err := retry.Run(func() error {
		err := f()
		if IsRetriableError(err) {
			return nil
		}
		return errors.WithStack(err)
	})
	return errors.WithStack(err)
}

// IsRetriableError returns true if the given error might be transient and the
// interaction can be safely retried.
func IsRetriableError(err error) bool {
	err = errors.Cause(err)

	if err == nil {
		return false
	}
	if err == sqlite3.ErrLocked || err == sqlite3.ErrBusy {
		return true
	}

	if strings.Contains(errors.Cause(err).Error(), "database is locked") {
		return true
	}
	if strings.Contains(errors.Cause(err).Error(), "bad connection") {
		return true
	}

	// Despite the description this is usually a lost leadership error.
	if strings.Contains(errors.Cause(err).Error(), "disk I/O error") {
		return true
	}

	return false
}
