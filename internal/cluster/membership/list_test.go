package membership_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/membership/mocks"
	"github.com/spoke-d/thermionic/internal/db"
)

func TestList(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockClock := mocks.NewMockClock(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(0), mockQuery)

		raftNode := []db.RaftNode{
			{
				ID:      0,
				Address: "10.0.0.0",
			},
		}
		nodeInfo := []db.NodeInfo{
			{
				ID:            0,
				Name:          "node1",
				Address:       "10.0.0.0",
				Description:   "node 1",
				Schema:        1,
				APIExtensions: 1,
				Heartbeat:     time.Now(),
			},
		}

		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		raftNodeMatcher := NodeDestSelectObjectsMatcher(raftNode)
		nodeInfoMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfo)

		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, raftNodeMatcher, gomock.Any()).Return(nil)

		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any(), 0).Return(nil)
		mockQuery.EXPECT().SelectStrings(mockTx, gomock.Any()).Return([]string{"5"}, nil)

		mockClock.EXPECT().Now().Return(time.Now()).Times(2)

		list := membership.NewList(
			mockState,
			membership.WithClockForList(mockClock),
		)
		members, err := list.Run()
		if err != nil {
			t.Errorf("expected err to be nil")
		}

		wanted := []membership.ClusterMember{
			{
				ServerName: "node1",
				URL:        "10.0.0.0",
				Database:   true,
				Status:     "online",
				Message:    "fully operational",
			},
		}

		if expected, actual := wanted, members; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if nodeMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
	})
}

func TestListWithNoNodes(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockClock := mocks.NewMockClock(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(0), mockQuery)

		raftNode := []db.RaftNode{
			{
				ID:      0,
				Address: "10.0.0.0",
			},
		}
		nodeInfo := []db.NodeInfo{}

		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		raftNodeMatcher := NodeDestSelectObjectsMatcher(raftNode)
		nodeInfoMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfo)

		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, raftNodeMatcher, gomock.Any()).Return(nil)

		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any(), 0).Return(nil)
		mockQuery.EXPECT().SelectStrings(mockTx, gomock.Any()).Return([]string{"5"}, nil)

		list := membership.NewList(
			mockState,
			membership.WithClockForList(mockClock),
		)
		members, err := list.Run()
		if err != nil {
			t.Errorf("expected err to be nil")
		}

		wanted := []membership.ClusterMember{}

		if expected, actual := wanted, members; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if nodeMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
	})
}

func TestListStatus(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, _ *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockClock := mocks.NewMockClock(ctrl)

		now := time.Now()

		mockClock.EXPECT().Now().Return(now).Times(4)

		nodeInfo := []db.NodeInfo{
			{
				ID:            0,
				Name:          "node1",
				Address:       "10.0.0.0",
				Description:   "node 1",
				Schema:        1,
				APIExtensions: 1,
				Heartbeat:     now.Add(-time.Minute),
			},
			{
				ID:            1,
				Name:          "node2",
				Address:       "10.0.0.1",
				Description:   "node 2",
				Schema:        2,
				APIExtensions: 0,
				Heartbeat:     now,
			},
			{
				ID:            2,
				Name:          "node3",
				Address:       "10.0.0.2",
				Description:   "node 3",
				Schema:        3,
				APIExtensions: 3,
				Heartbeat:     now,
			},
		}
		addresses := []string{
			"10.0.0.0",
			"10.0.0.1",
			"10.0.0.2",
		}

		list := membership.NewList(
			mockState,
			membership.WithClockForList(mockClock),
		)
		members, err := membership.ListStatus(list, nodeInfo, addresses, time.Second*5)
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		wanted := []membership.ClusterMember{
			{
				ServerName: "node1",
				URL:        "10.0.0.0",
				Database:   true,
				Status:     "offline",
				Message: fmt.Sprintf(
					"no heartbeat since %s", now.Sub(nodeInfo[0].Heartbeat).Round(time.Millisecond),
				),
			},
			{
				ServerName: "node2",
				URL:        "10.0.0.1",
				Database:   true,
				Status:     "broken",
				Message:    "inconsistent version",
			},
			{
				ServerName: "node3",
				URL:        "10.0.0.2",
				Database:   true,
				Status:     "blocked",
				Message:    "waiting for other nodes to be upgraded",
			},
		}

		if expected, actual := wanted, members; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}
