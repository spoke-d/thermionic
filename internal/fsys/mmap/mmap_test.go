package mmap

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
)

func TestOpen(t *testing.T) {
	const filename = "mmap_test.go"
	r, err := Open(filename)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	actual := make([]byte, r.Len())
	if _, err := r.ReadAt(actual, 0); err != nil && err != io.EOF {
		t.Fatalf("ReadAt: %v", err)
	}

	expected, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatalf("ioutil.ReadFile: %v", err)
	}
	if len(actual) != len(expected) {
		t.Fatalf("actual %d bytes, expected %d", len(actual), len(expected))
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("\nactual  %q\nexpected %q", string(actual), string(expected))
	}
}
