package query_test

import (
	"testing"

	"github.com/pkg/errors"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/spoke-d/thermionic/internal/db/query"
)

func TestIsRetriableError(t *testing.T) {
	ok := query.IsRetriableError(errors.New("bad"))
	if ok {
		t.Errorf("expected ok to be false")
	}
}

func TestIsRetriableErrorWithNil(t *testing.T) {
	ok := query.IsRetriableError(nil)
	if ok {
		t.Errorf("expected ok to be false")
	}
}

func TestIsRetriableErrorWithSQLite3Err(t *testing.T) {
	ok := query.IsRetriableError(sqlite3.ErrLocked)
	if !ok {
		t.Errorf("expected ok to be true")
	}
}

func TestIsRetriableErrorWithDatabaseLocked(t *testing.T) {
	ok := query.IsRetriableError(errors.New("database is locked"))
	if !ok {
		t.Errorf("expected ok to be true")
	}
}

func TestIsRetriableErrorWithBadConnection(t *testing.T) {
	ok := query.IsRetriableError(errors.New("bad connection"))
	if !ok {
		t.Errorf("expected ok to be true")
	}
}

func TestIsRetriableErrorWithDiskIOError(t *testing.T) {
	ok := query.IsRetriableError(errors.New("disk I/O error"))
	if !ok {
		t.Errorf("expected ok to be true")
	}
}
