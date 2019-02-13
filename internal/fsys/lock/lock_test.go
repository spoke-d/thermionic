package lock

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
)

func TestLocking(t *testing.T) {
	dir := newTemporaryDirectory("test_lock")
	defer dir.Close()

	fileName := filepath.Join(dir.Path(), "LOCK")

	if _, err := os.Stat(fileName); err == nil {
		t.Fatalf("File %q unexpectedly exists.", fileName)
	}

	lock, existed, err := New(fileName)
	if err != nil {
		t.Fatalf("Error locking file %q: %s", fileName, err)
	}
	if existed {
		t.Errorf("File %q reported as existing during locking.", fileName)
	}

	// File must now exist.
	if _, err = os.Stat(fileName); err != nil {
		t.Errorf("Could not stat file %q expected to exist: %s", fileName, err)
	}

	// Try to lock again.
	lockedAgain, existed, err := New(fileName)
	if err == nil {
		t.Fatalf("File %q locked twice.", fileName)
	}
	if lockedAgain != nil {
		t.Error("Unsuccessful locking did not return nil.")
	}
	if !existed {
		t.Errorf("Existing file %q not recognized.", fileName)
	}

	if err := lock.Release(); err != nil {
		t.Errorf("Error releasing lock for file %q: %s", fileName, err)
	}

	// File must still exist.
	if _, err = os.Stat(fileName); err != nil {
		t.Errorf("Could not stat file %q expected to exist: %s", fileName, err)
	}

	// Lock existing file.
	lock, existed, err = New(fileName)
	if err != nil {
		t.Fatalf("Error locking file %q: %s", fileName, err)
	}
	if !existed {
		t.Errorf("Existing file %q not recognized.", fileName)
	}

	if err := lock.Release(); err != nil {
		t.Errorf("Error releasing lock for file %q: %s", fileName, err)
	}
}

const (
	defaultDirectory                       = ""
	defaultTemporaryDirectoryRemoveRetries = 2
)

type temporaryDirectory struct {
	path string
}

func newTemporaryDirectory(name string) temporaryDirectory {
	directory, err := ioutil.TempDir(defaultDirectory, name)
	if err != nil {
		log.Fatal(err)
	}

	return temporaryDirectory{
		path: directory,
	}
}

func (t temporaryDirectory) Close() {
	var (
		retries = defaultTemporaryDirectoryRemoveRetries
		err     = os.RemoveAll(t.path)
	)
	for err != nil && retries > 0 {
		switch {
		case os.IsNotExist(err):
			err = nil
		default:
			retries--
			err = os.RemoveAll(t.path)
		}
	}
	if err != nil {
		log.Fatal(err)
	}
}

func (t temporaryDirectory) Path() string {
	return t.path
}
