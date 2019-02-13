package endpoints_test

import (
	"testing"

	"github.com/spoke-d/thermionic/internal/endpoints"
)

// Test network address is set, a new network TCP socket will be created.
func TestEndpoints_NetworkCreateTCPSocket(t *testing.T) {
	endpoints, cleanup := newEndpoints(t, endpoints.WithNetworkAddress("127.0.0.1:0"))
	defer cleanup()

	if err := endpoints.Up(); err != nil {
		t.Fatal(err)
	}

	err := httpGetOverTLSSocket(endpoints.NetworkAddress(), endpoints.NetworkCert())
	if expected, actual := true, err == nil; expected != actual {
		t.Errorf("expected: %t, actual: %t, err: %+v", expected, actual, err)
	}
}
