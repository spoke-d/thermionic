package net_test

import (
	"io"
	"reflect"
	"testing"

	"github.com/spoke-d/thermionic/internal/net"
)

// The connection returned by the dialer is paired with the one returned by the
// Accept() method of the listener.
func TestInMemoryNetwork(t *testing.T) {
	listener, dialer := net.InMemoryNetwork()
	client := dialer()
	server, err := listener.Accept()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	go client.Write([]byte("hello"))
	buffer := make([]byte, 5)
	n, err := server.Read(buffer)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := 5, n; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := []byte("hello"), buffer; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	// Closing the server makes all further client reads and
	// writes fail.
	server.Close()
	_, err = client.Read(buffer)
	if expected, actual := io.EOF, err; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	_, err = client.Write([]byte("hello"))
	if expected, actual := "io: read/write on closed pipe", err.Error(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}
