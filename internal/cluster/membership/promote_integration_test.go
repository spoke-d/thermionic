// +build integration

package membership_test

import (
	"net/http"
	"testing"
	"time"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/spoke-d/thermionic/internal/cluster"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/db"
	querycluster "github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/node"
	st "github.com/spoke-d/thermionic/internal/state"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
)

func TestNewPromote(t *testing.T) {
	certInfo := libtesting.KeyPair()

	var targetGateway *cluster.Gateway
	var targetAddress string
	var targetFixture *libtesting.MembershipFixtures

	// Setup a target node running as leader of a cluster.
	{
		mux := http.NewServeMux()
		server := libtesting.NewTestServer(t, certInfo, mux)
		defer server.Close()

		state, cleanup := libtesting.NewTestState(t, 1, 1)
		defer cleanup()

		gateway := libtesting.NewTestGateway(t, state.Node(), certInfo)
		defer gateway.Shutdown()

		for path, handler := range gateway.HandlerFuncs() {
			mux.HandleFunc(path, handler)
		}

		if err := state.Cluster().Close(); err != nil {
			t.Error(err)
		}

		fs := libtesting.NewFileSystem(t)
		address := server.Listener.Addr().String()

		cluster := db.NewCluster(querycluster.New(
			querycluster.NewBasicAPIExtensions(1),
			querycluster.NewSchema(fs),
		))
		if err := cluster.Open(
			gateway.ServerStore(),
			address, "/unused/db/dir",
			10*time.Second,
			dqlite.WithDialFunc(gateway.DialFunc()),
		); err != nil {
			t.Error(err)
		}
		st.UnsafeSetCluster(state, cluster)

		fixture := libtesting.NewMembershipFixtures(t, state)
		fixture.NetworkAddress(address)

		bootstrapTask := membership.NewBootstrap(
			makeMembershipStateShim(state),
			makeMembershipGatewayShim(gateway),
			certInfo, node.ConfigSchema,
		)
		if err := bootstrapTask.Run("buzz"); err != nil {
			t.Error(err)
		}

		targetGateway = gateway
		targetAddress = address
		targetFixture = fixture
	}

	// Setup a node to be promoted.
	mux := http.NewServeMux()
	server := libtesting.NewTestServer(t, certInfo, mux)
	defer server.Close()

	state, cleanup := libtesting.NewTestState(t, 1, 1)
	defer cleanup()

	address := server.Listener.Addr().String()
	targetFixture.ClusterNode(address, 1, 1)

	fixture := libtesting.NewMembershipFixtures(t, state)
	fixture.NetworkAddress(address)
	fixture.RaftNode(targetAddress)

	gateway := libtesting.NewTestGateway(t, state.Node(), certInfo)
	defer gateway.Shutdown()

	for path, handler := range gateway.HandlerFuncs() {
		mux.HandleFunc(path, handler)
	}

	// Promote the node
	fixture.UpdateClusterNode(1, "buzzer", address)
	targetFixture.RaftNode(address)
	raftNodes := targetFixture.RaftNodes()
	promoteTask := membership.NewPromote(
		makeMembershipStateShim(state),
		makeMembershipGatewayShim(gateway),
	)
	if err := promoteTask.Run(certInfo, raftNodes); err != nil {
		t.Error(err)
	}

	// The leader now returns an updated list of raft nodes.
	raftNodes, err := targetGateway.RaftNodes()
	if err != nil {
		t.Error(err)
	}
	if expected, actual := 2, len(raftNodes); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := int64(1), raftNodes[0].ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := targetAddress, raftNodes[0].Address; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := int64(2), raftNodes[1].ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := address, raftNodes[1].Address; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	// The List function returns all nodes in the cluster.
	listTask := membership.NewList(makeMembershipStateShim(state))
	nodes, err := listTask.Run()
	if err != nil {
		t.Error(err)
	}
	// TODO (Simon): This should be 2, the promote kills the other node
	if expected, actual := 1, len(nodes); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "online", nodes[0].Status; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := true, nodes[0].Database; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}
