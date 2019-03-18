// +build integration

package cluster_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/go-kit/kit/log"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/node"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
)

// Basic creation and shutdown. By default, the gateway runs an in-memory gRPC
// server.
func TestGateway_Single(t *testing.T) {
	db, cleanup := NewTestNode(t)
	defer cleanup()

	cert := libtesting.KeyPair()
	gateway := newGateway(t, db, cert)
	defer gateway.Shutdown()

	handlerFuncs := gateway.HandlerFuncs()
	if expected, actual := 2, len(handlerFuncs); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	for endpoint, f := range handlerFuncs {
		c, err := x509.ParseCertificate(cert.KeyPair().Certificate[0])
		if err != nil {
			t.Errorf("expected err to be nil: %v", err)
		}
		w := httptest.NewRecorder()
		r := &http.Request{}
		r.TLS = &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{c},
		}
		f(w, r)
		if expected, actual := 404, w.Code; expected != actual {
			t.Errorf("expected: %v, actual: %v for: %v", expected, actual, endpoint)
		}
	}

	dial := gateway.DialFunc()
	conn, err := dial(context.Background(), "")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if conn == nil {
		t.Errorf("expected conn not to be nil")
	}

	leader, err := gateway.LeaderAddress()
	if expected, actual := "", leader; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "node is not clustered", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// If there's a network address configured, we expose the gRPC endpoint with
// an HTTP handler.
func TestGateway_SingleWithNetworkAddress(t *testing.T) {
	db, cleanup := NewTestNode(t)
	defer cleanup()

	cert := libtesting.KeyPair()
	mux := http.NewServeMux()
	server := newServer(cert, mux, log.NewNopLogger())
	defer server.Close()

	address := server.Listener.Addr().String()
	store := setRaftRole(t, db, address)

	gateway := newGateway(t, db, cert)
	defer gateway.Shutdown()

	for path, handler := range gateway.HandlerFuncs() {
		mux.HandleFunc(path, handler)
	}

	driver, err := dqlite.NewDriver(store,
		dqlite.WithDialFunc(gateway.DialFunc()),
		dqlite.WithLogFunc(cluster.DqliteLog(log.NewNopLogger())),
	)
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	conn, err := driver.Open("test.db")
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	err = conn.Close()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	leader, err := gateway.LeaderAddress()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	if expected, actual := address, leader; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// When networked, the grpc and raft endpoints requires the cluster
// certificate.
func TestGateway_NetworkAuth(t *testing.T) {
	db, cleanup := NewTestNode(t)
	defer cleanup()

	certInfo := libtesting.KeyPair()
	mux := http.NewServeMux()
	server := newServer(certInfo, mux, log.NewNopLogger())
	defer server.Close()

	address := server.Listener.Addr().String()
	setRaftRole(t, db, address)

	gateway := newGateway(t, db, certInfo)
	defer gateway.Shutdown()

	for path, handler := range gateway.HandlerFuncs() {
		mux.HandleFunc(path, handler)
	}

	// Make a request using a certificate different than the cluster one.
	config, err := cert.TLSClientConfig(libtesting.AltKeyPair())
	config.InsecureSkipVerify = true // Skip client-side verification
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: config,
		},
	}

	for path := range gateway.HandlerFuncs() {
		url := fmt.Sprintf("https://%s%s", address, path)
		response, err := client.Head(url)
		if err != nil {
			t.Errorf("expected err to be nil: %v", err)
		}
		if expected, actual := http.StatusForbidden, response.StatusCode; expected != actual {
			t.Errorf("expected: %v, actual: %v for: %v", expected, actual, url)
		}
	}
}

// RaftNodes returns an error if the underlying raft instance is not the leader.
func TestGateway_RaftNodesNotLeader(t *testing.T) {
	db, cleanup := NewTestNode(t)
	defer cleanup()

	cert := libtesting.KeyPair()
	mux := http.NewServeMux()
	server := newServer(cert, mux, log.NewNopLogger())
	defer server.Close()

	address := server.Listener.Addr().String()
	setRaftRole(t, db, address)

	gateway := newGateway(t, db, cert)
	defer gateway.Shutdown()

	// Get the node immediately, before the election has took place.
	_, err := gateway.RaftNodes()
	if expected, actual := raft.ErrNotLeader.Error(), err.Error(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

// Create a new test Gateway with the given parameters, and ensure no error
// happens.
func newGateway(t *testing.T, db cluster.Node, certInfo *cert.Info) *cluster.Gateway {
	err := os.Mkdir(filepath.Join(db.Dir(), "global"), 0755)
	if err != nil {
		t.Fatal(err)
	}

	fileSystem := fsys.NewLocalFileSystem(false)

	gateway := cluster.NewGateway(
		db,
		node.ConfigSchema,
		fileSystem,
		cluster.WithLogger(log.NewNopLogger()),
	)
	if err := gateway.Init(certInfo); err != nil {
		t.Fatal(err)
	}
	return gateway
}

// Set the core.https_address config key to the given address, and insert the
// address into the raft_nodes table.
//
// This effectively makes the node act as a database raft node.
func setRaftRole(t *testing.T, node *db.Node, address string) *dqlite.DatabaseServerStore {
	err := node.Transaction(func(tx *db.NodeTx) error {
		err := tx.UpdateConfig(map[string]string{"core.https_address": address})
		if err != nil {
			return err
		}
		_, err = tx.RaftNodeAdd(address)
		return err
	})
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}

	raw, err := database.RawSQLDatabase(node.DB())
	if err != nil {
		t.Fatal(err)
	}
	store := dqlite.NewServerStore(raw, "main", "raft_nodes", "address")
	return store
}

// Create a new test HTTP server configured with the given TLS certificate and
// using the given handler.
func newServer(certInfo *cert.Info, handler http.Handler, logger log.Logger) *httptest.Server {
	server := httptest.NewUnstartedServer(handler)
	server.TLS = cert.ServerTLSConfig(certInfo, logger)
	server.StartTLS()
	return server
}
