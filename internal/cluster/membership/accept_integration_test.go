// +build integration

package membership_test

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
)

// If pre-conditions are not met, a descriptive error is returned.
func TestAccept_UnmetPreconditions(t *testing.T) {
	cases := []struct {
		name    string
		address string
		schema  int
		api     int
		setup   func(*libtesting.MembershipFixtures)
		err     string
	}{
		{
			"buzz",
			"1.2.3.4:666",
			2,
			2,
			func(f *libtesting.MembershipFixtures) {},
			"clustering not enabled",
		},
		{
			"rusp",
			"1.2.3.4:666",
			2,
			2,
			func(f *libtesting.MembershipFixtures) {
				f.ClusterNode("5.6.7.8:666", 2, 2)
			},
			"cluster already has node with name rusp",
		},
		{
			"buzz",
			"5.6.7.8:666",
			2,
			2,
			func(f *libtesting.MembershipFixtures) {
				f.ClusterNode("5.6.7.8:666", 2, 2)
			},
			"cluster already has node with address 5.6.7.8:666",
		},
		{
			"buzz",
			"1.2.3.4:666",
			1,
			2,
			func(f *libtesting.MembershipFixtures) {
				f.ClusterNode("5.6.7.8:666", 2, 2)
			},
			fmt.Sprintf("schema version mismatch: cluster has %d", 2),
		},
		{
			"buzz",
			"1.2.3.4:666",
			2,
			3,
			func(f *libtesting.MembershipFixtures) {
				f.ClusterNode("5.6.7.8:666", 2, 1)
			},
			fmt.Sprintf("api version mismatch: cluster has %d", 1),
		},
	}
	for _, c := range cases {
		t.Run(c.err, func(t *testing.T) {
			state, cleanup := libtesting.NewTestState(t, c.api, c.schema)
			defer cleanup()

			cert := libtesting.KeyPair()
			gateway := libtesting.NewTestGateway(t, state.Node(), cert)
			defer gateway.Shutdown()

			c.setup(libtesting.NewMembershipFixtures(t, state))

			acceptTask := membership.NewAccept(
				makeMembershipStateShim(state),
				makeMembershipGatewayShim(gateway),
			)
			_, err := acceptTask.Run(c.name, c.address, c.schema, c.api)
			if expected, actual := c.err, errors.Cause(err).Error(); expected != actual {
				t.Errorf("expected: %q, actual: %q", expected, actual)
			}
		})
	}
}

// When a node gets accepted, it gets included in the raft nodes.
func TestNewAccept(t *testing.T) {
	state, cleanup := libtesting.NewTestState(t, 1, 1)
	defer cleanup()

	cert := libtesting.KeyPair()
	gateway := libtesting.NewTestGateway(t, state.Node(), cert)
	defer gateway.Shutdown()

	f := libtesting.NewMembershipFixtures(t, state)
	f.RaftNode("1.2.3.4:666")
	f.ClusterNode("1.2.3.4:666", 1, 1)

	acceptTask := membership.NewAccept(
		makeMembershipStateShim(state),
		makeMembershipGatewayShim(gateway),
	)
	nodes, err := acceptTask.Run("buzz", "5.6.7.8:666", 1, 1)
	if err != nil {
		t.Error(err)
	}
	if expected, actual := 2, len(nodes); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := int64(1), nodes[0].ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "1.2.3.4:666", nodes[0].Address; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := int64(2), nodes[1].ID; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
	if expected, actual := "5.6.7.8:666", nodes[1].Address; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}
