package testing

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	rafttest "github.com/CanonicalLtd/raft-test"
	"github.com/go-kit/kit/log"
	"github.com/hashicorp/raft"
	"github.com/spoke-d/thermionic/internal/cert"
	libcluster "github.com/spoke-d/thermionic/internal/cluster"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/schema"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/internal/state"
	"github.com/spoke-d/thermionic/internal/sys"
)

// NewTestState creates a state using mocks and testing infrastructure.
func NewTestState(t *testing.T, apiExtensions, schema int) (*state.State, func()) {
	node, nodeCleanup := NewTestNode(t)
	cluster, clusterCleanup := NewTestCluster(t, apiExtensions, schema)
	os, osCleanup := NewTestOS(t)

	return state.NewState(
			state.WithNode(node),
			state.WithCluster(cluster),
			state.WithOS(os),
		), func() {
			nodeCleanup()
			clusterCleanup()
			osCleanup()
		}
}

// NewTestNode creates a new Node for testing purposes, along with a function
// that can be used to clean it up when done.
func NewTestNode(t *testing.T) (*db.Node, func()) {
	dir, err := ioutil.TempDir("", "lxd-db-test-node-")
	if err != nil {
		t.Error(err)
	}

	fs := fsys.NewLocalFileSystem(false)
	node := db.NewNode(fs)
	if err := node.Open(dir, nil); err != nil {
		t.Error(err)
	}

	return node, func() {
		if err := node.Close(); err != nil {
			t.Error(err)
		}
		if err := fs.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}
}

// NewTestNodeTx returns a fresh NodeTx object, along with a function that can
// be called to cleanup state when done with it.
func NewTestNodeTx(t *testing.T) (*db.NodeTx, func()) {
	node, nodeCleanup := NewTestNode(t)

	tx, err := node.DB().Begin()
	if err != nil {
		t.Errorf("expected err to be nil: %v", err)
	}
	nodeTx := db.NewNodeTx(tx)

	cleanup := func() {
		err := tx.Commit()
		if err != nil {
			t.Errorf("expected err to be nil: %v", err)
		}
		nodeCleanup()
	}

	return nodeTx, cleanup
}

// NewFileSystem returns a new FileSystem
func NewFileSystem(t *testing.T) fsys.FileSystem {
	t.Helper()

	return fsys.NewLocalFileSystem(false)
}

// NewTestQueryCluster creates a new Cluster for testing purposes
func NewTestQueryCluster(t *testing.T, apiExtensions, schema int) *cluster.Cluster {
	t.Helper()

	fs := NewFileSystem(t)

	return cluster.New(
		cluster.NewBasicAPIExtensions(apiExtensions),
		NewSchemaProvider(t, schema),
		cluster.WithFileSystem(fs),
		cluster.WithNameProvider(&cluster.DQliteNameProvider{}),
	)
}

// NewTestCluster creates a test cluster for testing purposes
func NewTestCluster(t *testing.T, apiExtensions, schema int) (*db.Cluster, func()) {
	t.Helper()

	store, serverCleanup := newDqliteServer(t)

	log := newLogFunc(t)

	fs := fsys.NewLocalFileSystem(false)
	cluster := db.NewCluster(
		NewTestQueryCluster(t, apiExtensions, schema),
		db.WithFileSystemForCluster(fs),
		db.WithSleeperForCluster(fastSleeper{}),
	)
	if err := cluster.Open(
		store, "1", "/unused/db/dir", 5*time.Second,
		dqlite.WithDialFunc(func(ctx context.Context, address string) (net.Conn, error) {
			return net.Dial("unix", address)
		}),
		dqlite.WithConnectionTimeout(5*time.Second),
		dqlite.WithLogFunc(log),
	); err != nil {
		t.Fatal(err)
	}

	return cluster, func() {
		if err := cluster.Close(); err != nil {
			t.Error(err)
		}
		serverCleanup()
	}
}

// NewTestClusterTx returns a fresh ClusterTx object, along with a function that can
// be called to cleanup state when done with it.
func NewTestClusterTx(t *testing.T, apiExtensions, schema int) (*db.ClusterTx, func()) {
	cluster, clusterCleanup := NewTestCluster(t, apiExtensions, schema)

	var err error

	tx, err := db.UnsafeClusterDB(cluster).Begin()
	if err != nil {
		t.Fatalf("expected err to be nil: %v", err)
	}

	clusterTx := db.NewClusterTx(tx, db.UnsafeNodeID(cluster))
	cleanup := func() {
		err := tx.Commit()
		if err != nil {
			t.Errorf("expected err to be nil: %v", err)
		}
		clusterCleanup()
	}

	return clusterTx, cleanup
}

// NewTestOS creates a sys.OS with dummy test infrastructure.
func NewTestOS(t *testing.T) (*sys.OS, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "lxd-db-test-node-")
	if err != nil {
		t.Error(err)
	}
	if err := setupTestCerts(dir); err != nil {
		t.Error(err)
	}

	fs := fsys.NewLocalFileSystem(false)
	os := sys.New(dir, filepath.Join(dir, "cache"), filepath.Join(dir, "log"))
	if err := os.Init(fs); err != nil {
		t.Error(err)
	}

	return os, func() {
		if err := fs.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}
}

func setupTestCerts(dir string) error {
	_, filename, _, _ := runtime.Caller(0)
	deps := filepath.Join(filepath.Dir(filename), "..", "..", "test", "deps")
	for _, f := range []string{"server.crt", "server.key"} {
		if err := os.Symlink(filepath.Join(deps, f), filepath.Join(dir, f)); err != nil {
			return err
		}
	}
	return nil
}

func newLogFunc(t *testing.T) dqlite.LogFunc {
	t.Helper()

	return func(l dqlite.LogLevel, format string, a ...interface{}) {
		format = fmt.Sprintf("%s [DQLITE]: %s", l.String(), format)
		t.Logf(format, a...)
	}
}

var dqliteSerial = 0

// Create a new in-memory dqlite server.
//
// Return the newly created server store can be used to connect to it.
func newDqliteServer(t *testing.T) (cluster.ServerStore, func()) {
	t.Helper()

	listener, err := net.Listen("unix", "")
	if err != nil {
		t.Error(err)
	}

	address := listener.Addr().String()
	store, err := dqlite.DefaultServerStore(":memory:")
	if err != nil {
		t.Error(err)
	}

	if err := store.Set(context.Background(), []dqlite.ServerInfo{
		{
			Address: address,
		},
	}); err != nil {
		t.Error(err)
	}

	id := fmt.Sprintf("%d", dqliteSerial)
	dqliteSerial++
	registry := dqlite.NewRegistry(id)
	fsm := dqlite.NewFSM(registry)

	r, raftCleanup := rafttest.Server(t, fsm, rafttest.Transport(func(i int) raft.Transport {
		if expected, actual := i, 0; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		address := raft.ServerAddress(listener.Addr().String())
		_, transport := raft.NewInmemTransport(address)
		return transport
	}))

	server, err := dqlite.NewServer(
		r, registry, listener,
		dqlite.WithServerLogFunc(newLogFunc(t)),
	)
	if err != nil {
		t.Error(err)
	}

	return makeServerStore(store), func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		raftCleanup()
	}
}

type schemaProvider struct {
	t          *testing.T
	FileSystem fsys.FileSystem
	Amount     int
	schema     cluster.SchemaProvider
}

// NewSchemaProvider creates a schemaProvider with test infrastructure.
func NewSchemaProvider(t *testing.T, amount int) schemaProvider {
	fs := NewFileSystem(t)
	return schemaProvider{
		t:          t,
		FileSystem: fs,
		Amount:     amount,
		schema:     cluster.NewSchema(fs),
	}
}

func (p schemaProvider) Schema() cluster.Schema {
	schema := schema.New(p.FileSystem, p.Updates())
	schema.Fresh(cluster.FreshSchema())
	return schema
}

func (p schemaProvider) Updates() []schema.Update {
	if p.Amount < 1 {
		p.t.Fatalf("expected amount to be greater than 0, received: %d", p.Amount)
	}

	stmts := p.schema.Updates()
	if p.Amount <= len(stmts) {
		return stmts[:p.Amount]
	}

	for i := len(stmts); i < p.Amount; i++ {
		stmts = append(stmts, func(tx database.Tx) error {
			return nil
		})
	}
	return stmts
}

type fastSleeper struct{}

func (fastSleeper) Sleep(t time.Duration) {
	// We still want to sleep during the integration tests, but we don't want
	// to sleep for the entire duration. This just aims to improve the
	// responsive
	time.Sleep(t / 500)
}

// NewTestGateway creates a new test Gateway with the given parameters, and
// ensure no error happens.
func NewTestGateway(t *testing.T, db libcluster.Node, certInfo *cert.Info) *libcluster.Gateway {
	if err := os.Mkdir(filepath.Join(db.Dir(), "global"), 0755); err != nil {
		t.Error(err)
	}

	fs := fsys.NewLocalFileSystem(true)
	gateway := libcluster.NewGateway(
		db, node.ConfigSchema, fs,
		libcluster.WithLatency(0.2),
		libcluster.WithLogger(log.NewLogfmtLogger(&testingWriter{t: t})),
	)
	if err := gateway.Init(certInfo); err != nil {
		t.Error(err)
	}
	return gateway
}

// NewTestServer creates a new test HTTP server configured with the given TLS certificate and
// using the given handler.
func NewTestServer(t *testing.T, certInfo *cert.Info, handler http.Handler) *httptest.Server {
	server := httptest.NewUnstartedServer(handler)
	server.TLS = cert.ServerTLSConfig(certInfo, log.NewNopLogger())
	server.StartTLS()
	return server
}

type testingWriter struct {
	t *testing.T
}

func (w *testingWriter) Write(b []byte) (int, error) {
	w.t.Log(strings.TrimSuffix(string(b), "\n"))
	return len(b), nil
}

type serverStoreShim struct {
	store dqlite.ServerStore
}

func makeServerStore(store dqlite.ServerStore) serverStoreShim {
	return serverStoreShim{
		store: store,
	}
}

// Get return the list of known servers.
func (s serverStoreShim) Get(ctx context.Context) ([]cluster.ServerInfo, error) {
	info, err := s.store.Get(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]cluster.ServerInfo, len(info))
	for k, v := range info {
		res[k] = cluster.ServerInfo{
			ID:      v.ID,
			Address: v.Address,
		}
	}
	return res, nil
}

// Set updates the list of known cluster servers.
func (s serverStoreShim) Set(ctx context.Context, info []cluster.ServerInfo) error {
	res := make([]dqlite.ServerInfo, len(info))
	for k, v := range info {
		res[k] = dqlite.ServerInfo{
			ID:      v.ID,
			Address: v.Address,
		}
	}
	return s.store.Set(ctx, res)
}
