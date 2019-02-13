package config_test

import (
	"testing"

	"github.com/spoke-d/thermionic/internal/config"
)

// Errors can be sorted by key name, and the global error message mentions the
// first of them.
func TestErrorListWithError(t *testing.T) {
	var errors config.ErrorList
	errors.Add("foo", "xxx", "boom")
	errors.Add("bar", "yyy", "ugh")
	errors.Sort()

	wanted := "cannot set 'bar' to 'yyy': ugh (and 1 more errors)"
	if expected, actual := wanted, errors.Error(); expected != actual {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}
}

func TestErrorListWithNoError(t *testing.T) {
	var errors config.ErrorList
	errors.Sort()

	wanted := "no errors"
	if expected, actual := wanted, errors.Error(); expected != actual {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}
}

func TestErrorListWithOneError(t *testing.T) {
	var errors config.ErrorList
	errors.Add("foo", "xxx", "boom")
	errors.Sort()

	wanted := "cannot set 'foo' to 'xxx': boom"
	if expected, actual := wanted, errors.Error(); expected != actual {
		t.Errorf("expected: %q, actual: %q", expected, actual)
	}
}
