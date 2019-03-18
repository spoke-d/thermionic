package membership_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	clusterconfig "github.com/spoke-d/thermionic/internal/cluster/config"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/membership/mocks"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
)

func TestRebalance(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockNode := mocks.NewMockNode(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockResult := mocks.NewMockResult(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockConfigProvider := mocks.NewMockClusterConfigProvider(ctrl2)
		mockClock := mocks.NewMockClock(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		configMap, err := config.New(clusterconfig.Schema, map[string]string{
			"core.proxy_http":           "[::]:8443",
			"core.proxy_https":          "[::]:8443",
			"cluster.offline_threshold": "20",
		})
		if err != nil {
			t.Errorf("expected err to be nil")
		}

		raftNodes := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.1",
			},
		}
		clusterConfig := clusterconfig.NewConfig(
			configMap,
			clusterconfig.WithNodeTx(clusterTx),
		)
		nodeInfo := []db.NodeInfo{
			{
				ID:            int64(1),
				Name:          "node1",
				Address:       "10.0.0.1",
				APIExtensions: 1,
				Schema:        1,
				Heartbeat:     time.Now(),
			},
			{
				ID:            int64(2),
				Name:          "node2",
				Address:       "10.0.0.2",
				APIExtensions: 1,
				Schema:        1,
				Heartbeat:     time.Now(),
			},
		}
		nodeInfoMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfo)

		mockGateway.EXPECT().RaftNodes().Return(raftNodes, nil)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockConfigProvider.EXPECT().ConfigLoad(clusterTx, clusterconfig.Schema).Return(clusterConfig, nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any(), 0).Return(nil)
		mockClock.EXPECT().Now().Return(time.Now())

		// DB actions
		mockGateway.EXPECT().DB().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"address",
		}, []interface{}{
			"10.0.0.2",
		}).Return(int64(2), nil)
		mockTx.EXPECT().Exec(gomock.Any()).Return(mockResult, nil)
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id", "address",
		}, []interface{}{
			int64(1), "10.0.0.1",
		}).Return(int64(1), nil)
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id", "address",
		}, []interface{}{
			int64(2), "10.0.0.2",
		}).Return(int64(2), nil)

		rebalance := membership.NewRebalance(
			mockState, mockGateway,
			clusterconfig.Schema,
			membership.WithConfigForRebalance(mockConfigProvider),
			membership.WithClockForRebalance(mockClock),
		)
		address, nodes, err := rebalance.Run()
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		if expected, actual := "10.0.0.2", address; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
		if expected, actual := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.1",
			},
			{
				ID:      int64(2),
				Address: "10.0.0.2",
			},
		}, nodes; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestRebalanceWithMoreNodesThanQuorum(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)

		mockConfigProvider := mocks.NewMockClusterConfigProvider(ctrl2)
		mockClock := mocks.NewMockClock(ctrl2)

		raftNodes := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.1",
			},
			{
				ID:      int64(2),
				Address: "10.0.0.2",
			},
			{
				ID:      int64(3),
				Address: "10.0.0.3",
			},
		}

		mockGateway.EXPECT().RaftNodes().Return(raftNodes, nil)

		rebalance := membership.NewRebalance(
			mockState, mockGateway,
			clusterconfig.Schema,
			membership.WithConfigForRebalance(mockConfigProvider),
			membership.WithClockForRebalance(mockClock),
		)
		_, _, err := rebalance.Run()
		if err != nil {
			t.Errorf("expected err to be nil")
		}
	})
}

func TestRebalanceWithNoValidAddress(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockConfigProvider := mocks.NewMockClusterConfigProvider(ctrl2)
		mockClock := mocks.NewMockClock(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		configMap, err := config.New(clusterconfig.Schema, map[string]string{
			"core.proxy_http":           "[::]:8443",
			"core.proxy_https":          "[::]:8443",
			"cluster.offline_threshold": "20",
		})
		if err != nil {
			t.Errorf("expected err to be nil")
		}

		raftNodes := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.1",
			},
		}
		clusterConfig := clusterconfig.NewConfig(
			configMap,
			clusterconfig.WithNodeTx(clusterTx),
		)
		nodeInfo := []db.NodeInfo{
			{
				ID:            int64(1),
				Name:          "node1",
				Address:       "10.0.0.1",
				APIExtensions: 1,
				Schema:        1,
				Heartbeat:     time.Now(),
			},
			{
				ID:            int64(2),
				Name:          "node2",
				Address:       "10.0.0.2",
				APIExtensions: 1,
				Schema:        1,
				Heartbeat:     time.Now().Add(-time.Hour),
			},
		}
		nodeInfoMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfo)

		mockGateway.EXPECT().RaftNodes().Return(raftNodes, nil)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockConfigProvider.EXPECT().ConfigLoad(clusterTx, clusterconfig.Schema).Return(clusterConfig, nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any(), 0).Return(nil)
		mockClock.EXPECT().Now().Return(time.Now())

		rebalance := membership.NewRebalance(
			mockState, mockGateway,
			clusterconfig.Schema,
			membership.WithConfigForRebalance(mockConfigProvider),
			membership.WithClockForRebalance(mockClock),
		)
		_, _, err = rebalance.Run()
		if err != nil {
			t.Errorf("expected err to be nil")
		}
	})
}
