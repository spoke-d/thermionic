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

func TestNewJoin(t *testing.T) {
	certInfo := libtesting.KeyPair()
	altCertInfo := libtesting.AltKeyPair()

	var targetState *st.State
	var targetGateway *cluster.Gateway
	var targetAddress string

	// Setup a target node running as leader of a cluster.
	{
		mux := http.NewServeMux()
		server := libtesting.NewTestServer(t, certInfo, mux)
		defer server.Close()

		state, cleanup := libtesting.NewTestState(t, 1, 2)
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
			certInfo, node.ConfigSchema)
		if err := bootstrapTask.Run("buzz"); err != nil {
			t.Error(err)
		}

		targetState = state
		targetGateway = gateway
		targetAddress = address
	}

	// Setup a joining node
	mux := http.NewServeMux()
	server := libtesting.NewTestServer(t, certInfo, mux)
	defer server.Close()

	state, cleanup := libtesting.NewTestState(t, 1, 2)
	defer cleanup()

	gateway := libtesting.NewTestGateway(t, state.Node(), altCertInfo)
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
		5*time.Second,
		dqlite.WithDialFunc(gateway.DialFunc()),
	); err != nil {
		t.Error(err)
	}
	st.UnsafeSetCluster(state, cluster)

	fixture := libtesting.NewMembershipFixtures(t, state)
	fixture.NetworkAddress(address)

	// Accept the joining node.
	acceptTask := membership.NewAccept(
		makeMembershipStateShim(targetState),
		makeMembershipGatewayShim(targetGateway),
	)
	raftNodes, err := acceptTask.Run("rusp", address, 2, 1)
	if err != nil {
		t.Error(err)
	}

	// Actually join the cluster.
	joinTask := membership.NewJoin(
		makeMembershipStateShim(state),
		makeMembershipGatewayShim(gateway),
		node.ConfigSchema,
	)
	err = joinTask.Run(certInfo, "rusp", raftNodes)
	if err != nil {
		t.Error(err)
	}

	// The leader now returns an updated list of raft nodes.
	raftNodes, err = targetGateway.RaftNodes()
	if err != nil {
		t.Error(err)
	}
	if expected, actual := 2, len(raftNodes); expected != actual {
		t.Fatalf("expected: %v, actual: %v", expected, actual)
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
	if expected, actual := 2, len(nodes); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "online", nodes[0].Status; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := true, nodes[0].Database; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "online", nodes[1].Status; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := true, nodes[1].Database; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	// The Count function returns the number of nodes.
	countTask := membership.NewCount(makeMembershipStateShim(state))
	count, err := countTask.Run()
	if err != nil {
		t.Error(err)
	}
	if expected, actual := 2, count; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	// Leave the cluster.
	leaveTask := membership.NewLeave(
		makeMembershipStateShim(state),
		makeMembershipGatewayShim(gateway),
	)
	leaving, err := leaveTask.Run("rusp", false)
	if err != nil {
		t.Error(err)
	}
	if expected, actual := address, leaving; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	purgeTask := membership.NewPurge(makeMembershipStateShim(state))
	err = purgeTask.Run("rusp")
	if err != nil {
		t.Error(err)
	}

	// The node has gone from the cluster db.
	err = targetState.Cluster().Transaction(func(tx *db.ClusterTx) error {
		nodes, err := tx.Nodes()
		if err != nil {
			t.Error(err)
		}
		if expected, actual := 1, len(nodes); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}

	// The node has gone from the raft cluster.
	raft := targetGateway.Raft()
	future := raft.Raft().GetConfiguration()
	if err := future.Error(); err != nil {
		t.Error(err)
	}
	if expected, actual := 1, len(future.Configuration().Servers); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	count, err = countTask.Run()
	if err != nil {
		t.Error(err)
	}
	if expected, actual := 1, count; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}
