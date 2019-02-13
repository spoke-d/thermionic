// +build integration

package heartbeat_test

import (
	"context"
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/clock"
	"github.com/spoke-d/thermionic/internal/cluster/heartbeat"
	"github.com/spoke-d/thermionic/internal/db"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
)

// After a heartbeat request is completed, the leader updates the heartbeat
// timestamp column, and the serving node updates its cache of raft nodes.
func TestNewHeartbeat(t *testing.T) {
	f := libtesting.NewFixtures(t)
	defer f.Cleanup()

	f.Bootstrap()
	f.Grow()
	f.Grow()

	leader := f.Leader()
	leaderState := f.State(leader)

	// Artificially mark all nodes as down
	err := leaderState.Cluster().Transaction(func(tx *db.ClusterTx) error {
		nodes, err := tx.Nodes()
		if err != nil {
			t.Error(err)
		}
		for _, node := range nodes {
			err := tx.NodeHeartbeat(node.Address, time.Now().Add(-time.Minute))
			if err != nil {
				t.Error(err)
			}
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}

	// Perform the heartbeat requests.
	heartbeatTask := heartbeat.New(
		libtesting.NewHeartbeatGatewayShim(leader),
		leaderState.Cluster(),
		heartbeat.DatabaseEndpoint,
	)
	heartbeat, _ := heartbeatTask.Run()
	ctx := context.Background()
	heartbeat(ctx)

	// The heartbeat timestamps of all nodes got updated
	err = leaderState.Cluster().Transaction(func(tx *db.ClusterTx) error {
		nodes, err := tx.Nodes()
		if err != nil {
			t.Error(err)
		}

		offlineThreshold, err := tx.NodeOfflineThreshold()
		if err != nil {
			t.Error(err)
		}

		for _, node := range nodes {
			offline := node.IsOffline(clock.New(), offlineThreshold)
			if expected, actual := false, offline; expected != actual {
				t.Errorf("expected: %v, actual: %v", expected, actual)
			}
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}
