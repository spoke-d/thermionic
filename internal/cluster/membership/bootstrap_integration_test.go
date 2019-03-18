// +build integration

package membership_test

import (
	"io/ioutil"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/fsys"
	"github.com/spoke-d/thermionic/internal/node"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
)

func TestBootstrap_UnmetPreconditions(t *testing.T) {
	cases := []struct {
		setup func(*libtesting.MembershipFixtures)
		err   string
	}{
		{
			setup: func(f *libtesting.MembershipFixtures) {
				f.NetworkAddress("1.2.3.4:666")
				filename := filepath.Join(f.State().OS().VarDir(), "cluster.crt")
				ioutil.WriteFile(filename, []byte{}, 0644)
			},
			err: "inconsistent state: found leftover cluster certificate",
		},
		{
			setup: func(*libtesting.MembershipFixtures) {},
			err:   "no core.https_address config is set on this node",
		},
		{
			setup: func(f *libtesting.MembershipFixtures) {
				f.NetworkAddress("1.2.3.4:666")
				f.RaftNode("5.6.7.8:666")
			},
			err: "the node is already part of a cluster",
		},
		{
			setup: func(f *libtesting.MembershipFixtures) {
				f.RaftNode("5.6.7.8:666")
			},
			err: "inconsistent state: found leftover entries in raft_nodes",
		},
		{
			setup: func(f *libtesting.MembershipFixtures) {
				f.NetworkAddress("1.2.3.4:666")
				f.ClusterNode("5.6.7.8:666", 1, 1)
			},
			err: "inconsistent state: found leftover entries in nodes",
		},
	}

	for _, c := range cases {
		t.Run(c.err, func(t *testing.T) {
			state, cleanup := libtesting.NewTestState(t, 1, 1)
			defer cleanup()

			c.setup(libtesting.NewMembershipFixtures(t, state))

			cert := libtesting.KeyPair()
			gateway := libtesting.NewTestGateway(t, state.Node(), cert)
			defer gateway.Shutdown()

			fs := fsys.NewLocalFileSystem(true)

			bootstrapTask := membership.NewBootstrap(
				makeMembershipStateShim(state),
				makeMembershipGatewayShim(gateway),
				cert, node.ConfigSchema,
				membership.WithFileSystemForBootstrap(fs),
			)
			err := bootstrapTask.Run("buzz")
			if expected, actual := c.err, errors.Cause(err).Error(); expected != actual {
				t.Errorf("expected: %q, actual: %q", expected, actual)
			}
		})
	}
}

func TestNewBootstrap(t *testing.T) {
	state, cleanup := libtesting.NewTestState(t, 1, 1)
	defer cleanup()

	cert := libtesting.KeyPair()
	gateway := libtesting.NewTestGateway(t, state.Node(), cert)
	defer gateway.Shutdown()

	mux := http.NewServeMux()
	server := libtesting.NewTestServer(t, cert, mux)
	defer server.Close()

	address := server.Listener.Addr().String()

	f := libtesting.NewMembershipFixtures(t, state)
	f.NetworkAddress(address)

	fs := fsys.NewLocalFileSystem(true)

	bootstrapTask := membership.NewBootstrap(
		makeMembershipStateShim(state),
		makeMembershipGatewayShim(gateway),
		cert, node.ConfigSchema,
		membership.WithFileSystemForBootstrap(fs),
	)
	if err := bootstrapTask.Run("buzz"); err != nil {
		t.Error(err)
	}

	// The node-local database has now an entry in the raft_nodes table
	if err := state.Node().Transaction(func(tx *db.NodeTx) error {
		nodes, err := tx.RaftNodes()
		if err != nil {
			t.Error(err)
		}
		if expected, actual := 1, len(nodes); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := int64(1), nodes[0].ID; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := address, nodes[0].Address; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}

	// The cluster database has now an entry in the nodes table
	if err := state.Cluster().Transaction(func(tx *db.ClusterTx) error {
		nodes, err := tx.Nodes()
		if err != nil {
			t.Error(err)
		}
		if expected, actual := 1, len(nodes); expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := "buzz", nodes[0].Name; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := address, nodes[0].Address; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}

	// The cluster certificate is in place.
	if expected, actual := true, fs.Exists(filepath.Join(state.OS().VarDir(), "cluster.crt")); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	// The dqlite driver is now exposed over the network.
	for path, handler := range gateway.HandlerFuncs() {
		mux.HandleFunc(path, handler)
	}

	count := membership.NewCount(
		makeMembershipStateShim(state),
	)
	c, err := count.Run()
	if err != nil {
		t.Error(err)
	}
	if expected, actual := 1, c; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}

	enabled := membership.NewEnabled(
		makeMembershipStateShim(state),
	)
	b, err := enabled.Run()
	if err != nil {
		t.Error(err)
	}
	if expected, actual := true, b; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}
