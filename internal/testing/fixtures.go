package testing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/hashicorp/raft"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster"
	"github.com/spoke-d/thermionic/internal/cluster/heartbeat"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/db"
	querycluster "github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/spoke-d/thermionic/internal/state"
	st "github.com/spoke-d/thermionic/internal/state"
)

// Fixtures are a series of helper for integration tests.
type Fixtures struct {
	t        *testing.T
	gateways map[int]*cluster.Gateway              // node index to gateway
	states   map[*cluster.Gateway]*state.State     // gateway to its state handle
	servers  map[*cluster.Gateway]*httptest.Server // gateway to its HTTP server
	cleanups []func()
}

// NewFixtures creates a series of fixtures with sane defaults
func NewFixtures(t *testing.T) *Fixtures {
	return &Fixtures{
		t:        t,
		gateways: make(map[int]*cluster.Gateway),
		states:   make(map[*cluster.Gateway]*state.State),
		servers:  make(map[*cluster.Gateway]*httptest.Server),
	}
}

// Bootstrap the first node of the cluster.
func (f *Fixtures) Bootstrap() *cluster.Gateway {
	f.t.Logf("create bootstrap node for test cluster")
	state, gateway, _ := f.node()

	certInfo := KeyPair()

	bootstrapTask := membership.NewBootstrap(
		makeMembershipStateShim(state),
		makeMembershipGatewayShim(gateway),
		certInfo, node.ConfigSchema,
	)
	if err := bootstrapTask.Run("buzz"); err != nil {
		f.t.Fatal(err)
	}
	return gateway
}

// Grow adds a new node to the cluster.
func (f *Fixtures) Grow() *cluster.Gateway {
	// Figure out the current leader
	f.t.Logf("adding another node to the test cluster")
	target := f.Leader()
	targetState := f.states[target]

	state, gateway, address := f.node()
	name := address

	acceptTask := membership.NewAccept(
		makeMembershipStateShim(targetState),
		makeMembershipGatewayShim(target),
	)
	nodes, err := acceptTask.Run(name, address, 1, 1)
	if err != nil {
		f.t.Fatal(err)
	}

	joinTask := membership.NewJoin(
		makeMembershipStateShim(state),
		makeMembershipGatewayShim(gateway),
		node.ConfigSchema,
	)
	if err := joinTask.Run(target.Cert(), name, nodes); err != nil {
		f.t.Fatal(err)
	}

	return gateway
}

// Leader returns the leader gateway in the cluster.
func (f *Fixtures) Leader() *cluster.Gateway {
	timeout := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		for _, gateway := range f.gateways {
			if gateway.Raft().Raft().State() == raft.Leader {
				return gateway
			}
		}

		select {
		case <-ctx.Done():
			f.t.Fatalf("no leader was elected within %s", timeout)
		default:
		}

		// Wait a bit for election to take place
		time.Sleep(10 * time.Millisecond)
	}
}

// Follower returns a follower gateway in the cluster.
func (f *Fixtures) Follower() *cluster.Gateway {
	timeout := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		for _, gateway := range f.gateways {
			if gateway.Raft().Raft().State() == raft.Follower {
				return gateway
			}
		}

		select {
		case <-ctx.Done():
			f.t.Fatalf("no node running as follower")
		default:
		}

		// Wait a bit for election to take place
		time.Sleep(10 * time.Millisecond)
	}
}

// Index returns the cluster index of the given gateway.
func (f *Fixtures) Index(gateway *cluster.Gateway) int {
	for i := range f.gateways {
		if f.gateways[i] == gateway {
			return i
		}
	}
	return -1
}

// State returns the state associated with the given gateway.
func (f *Fixtures) State(gateway *cluster.Gateway) *state.State {
	return f.states[gateway]
}

// Server returns the HTTP server associated with the given gateway.
func (f *Fixtures) Server(gateway *cluster.Gateway) *httptest.Server {
	return f.servers[gateway]
}

// Cleanup runs any potential clean ups
func (f *Fixtures) Cleanup() {
	// Run the cleanups in reverse order
	for i := len(f.cleanups) - 1; i >= 0; i-- {
		f.cleanups[i]()
	}
	for _, server := range f.servers {
		server.Close()
	}
}

// Creates a new node, without either bootstrapping or joining it.
//
// Return the Fixtures gateway and network address.
func (f *Fixtures) node() (*state.State, *cluster.Gateway, string) {
	state, cleanup := NewTestState(f.t, 1, 1)
	f.cleanups = append(f.cleanups, cleanup)

	certInfo := KeyPair()
	gateway := NewTestGateway(f.t, state.Node(), certInfo)
	f.cleanups = append(f.cleanups, func() { gateway.Shutdown() })

	mux := http.NewServeMux()
	server := NewTestServer(f.t, certInfo, mux)

	for path, handler := range gateway.HandlerFuncs() {
		mux.HandleFunc(path, handler)
	}

	address := server.Listener.Addr().String()
	mf := NewMembershipFixtures(f.t, state)
	mf.NetworkAddress(address)

	if err := state.Cluster().Close(); err != nil {
		f.t.Fatal(err)
	}

	fs := NewFileSystem(f.t)

	cluster := db.NewCluster(querycluster.New(
		querycluster.NewBasicAPIExtensions(1),
		querycluster.NewSchema(fs),
	))
	if err := cluster.Open(
		gateway.ServerStore(),
		address, "/unused/db/dir",
		5*time.Second,
		dqlite.WithDialFunc(gateway.DialFunc()),
	); err != nil {
		f.t.Error(err)
	}
	st.UnsafeSetCluster(state, cluster)

	f.gateways[len(f.gateways)] = gateway
	f.states[gateway] = state
	f.servers[gateway] = server

	return state, gateway, address
}

// MembershipFixtures are helpers for integration tests.
type MembershipFixtures struct {
	t     *testing.T
	state *state.State
}

// NewMembershipFixtures will create a new MembershipFixtures with sane defaults
func NewMembershipFixtures(t *testing.T, state *state.State) *MembershipFixtures {
	return &MembershipFixtures{
		t:     t,
		state: state,
	}
}

// State returns the associated state
func (h *MembershipFixtures) State() *state.State {
	return h.state
}

// NetworkAddress sets core.https_address to the given value.
func (h *MembershipFixtures) NetworkAddress(address string) {
	if err := h.state.Node().Transaction(func(tx *db.NodeTx) error {
		config := map[string]string{
			"core.https_address": address,
		}
		return tx.UpdateConfig(config)
	}); err != nil {
		h.t.Fatal(err)
	}
}

// RaftNode will add the given address to the raft_nodes table.
func (h *MembershipFixtures) RaftNode(address string) {
	if err := h.state.Node().Transaction(func(tx *db.NodeTx) error {
		_, err := tx.RaftNodeAdd(address)
		return err
	}); err != nil {
		h.t.Fatal(err)
	}
}

// ClusterNode will add the given address to the nodes table of the cluster database.
func (h *MembershipFixtures) ClusterNode(address string, schema, api int) {
	if err := h.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		_, err := tx.NodeAdd("rusp", address, schema, api)
		return err
	}); err != nil {
		h.t.Fatal(err)
	}
}

// UpdateClusterNode will update a given cluster node.
func (h *MembershipFixtures) UpdateClusterNode(id int64, name, address string) {
	if err := h.state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		err := tx.NodeUpdate(id, name, address)
		return err
	}); err != nil {
		h.t.Fatal(err)
	}
}

// RaftNodes will get the current list of the raft nodes in the raft_nodes table.
func (h *MembershipFixtures) RaftNodes() []db.RaftNode {
	var nodes []db.RaftNode
	if err := h.state.Node().Transaction(func(tx *db.NodeTx) error {
		var err error
		nodes, err = tx.RaftNodes()
		return err
	}); err != nil {
		h.t.Fatal(err)
	}
	return nodes
}

type membershipStateShim struct {
	state *state.State
}

func makeMembershipStateShim(state *state.State) membershipStateShim {
	return membershipStateShim{
		state: state,
	}
}

func (s membershipStateShim) Node() membership.Node {
	return s.state.Node()
}

func (s membershipStateShim) Cluster() membership.Cluster {
	return s.state.Cluster()
}

func (s membershipStateShim) OS() membership.OS {
	return s.state.OS()
}

type membershipGatewayShim struct {
	gateway *cluster.Gateway
}

func makeMembershipGatewayShim(gateway *cluster.Gateway) membershipGatewayShim {
	return membershipGatewayShim{
		gateway: gateway,
	}
}

func (s membershipGatewayShim) Init(certInfo *cert.Info) error {
	return s.gateway.Init(certInfo)
}

func (s membershipGatewayShim) Shutdown() error {
	return s.gateway.Shutdown()
}

func (s membershipGatewayShim) WaitLeadership() error {
	return s.gateway.WaitLeadership()
}

func (s membershipGatewayShim) RaftNodes() ([]db.RaftNode, error) {
	return s.gateway.RaftNodes()
}

func (s membershipGatewayShim) Raft() membership.RaftInstance {
	return s.gateway.Raft()
}

func (s membershipGatewayShim) DB() membership.Node {
	return s.gateway.DB()
}

func (s membershipGatewayShim) IsDatabaseNode() bool {
	return s.gateway.IsDatabaseNode()
}

func (s membershipGatewayShim) Clustered() bool {
	return s.gateway.Clustered()
}

func (s membershipGatewayShim) Cert() *cert.Info {
	return s.gateway.Cert()
}

func (s membershipGatewayShim) Reset(certInfo *cert.Info) error {
	return s.gateway.Reset(certInfo)
}

func (s membershipGatewayShim) DialFunc() dqlite.DialFunc {
	return s.gateway.DialFunc()
}

func (s membershipGatewayShim) ServerStore() querycluster.ServerStore {
	return s.gateway.ServerStore()
}

func (s membershipGatewayShim) Context() context.Context {
	return s.gateway.Context()
}

type heartbeatGatewayShim struct {
	gateway *cluster.Gateway
}

// NewHeartbeatGatewayShim creates a new heartbeat gateway shim
func NewHeartbeatGatewayShim(gateway *cluster.Gateway) heartbeat.Gateway {
	return heartbeatGatewayShim{
		gateway: gateway,
	}
}

func (s heartbeatGatewayShim) RaftNodes() ([]db.RaftNode, error) {
	return s.gateway.RaftNodes()
}

func (s heartbeatGatewayShim) DB() heartbeat.Node {
	return s.gateway.DB()
}

func (s heartbeatGatewayShim) Clustered() bool {
	return s.gateway.Clustered()
}

func (s heartbeatGatewayShim) Cert() *cert.Info {
	return s.gateway.Cert()
}
