package membership_test

import (
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/membership/mocks"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/node"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"

	"github.com/golang/mock/gomock"
)

func TestJoin(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockRaftInstance := mocks.NewMockRaftInstance(ctrl)
		mockRaftChanger := mocks.NewMockChanger(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockResult := mocks.NewMockResult(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockConfigProvider := mocks.NewMockNodeConfigProvider(ctrl2)
		mockOS := mocks.NewMockOS(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		certInfo := &cert.Info{}
		configMap, err := config.New(node.ConfigSchema, map[string]string{
			"core.https_address": "[::]:8443",
		})
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		nodeConfig := node.NewConfig(
			node.NodeTx(nodeTx),
			node.Map(configMap),
		)
		existingRaftNodes := []db.RaftNode{}
		newRaftNodes := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "[::]:8443",
			},
		}
		raftNodeMatcher := NodeDestSelectObjectsMatcher(existingRaftNodes)
		operations := []db.Operation{
			{
				ID:          0,
				UUID:        "15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
				NodeAddress: "0.0.0.0",
				Type:        db.OperationType("lock"),
			},
		}
		operationsMatcher := OperationDestSelectObjectsMatcher(operations)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
		mockConfigProvider.EXPECT().ConfigLoad(nodeTx, node.ConfigSchema).Return(nodeConfig, nil)
		mockQuery.EXPECT().SelectObjects(mockTx, raftNodeMatcher, gomock.Any()).Return(nil)
		mockTx.EXPECT().Exec(gomock.Any()).Return(mockResult, nil)
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id", "address",
		}, []interface{}{
			int64(1), "[::]:8443",
		}).Return(int64(1), nil)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, operationsMatcher, gomock.Any(), int64(1)).Return(nil)

		// Cluster leadership actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().EnterExclusive().Return(nil)
		mockGateway.EXPECT().Shutdown().Return(nil)

		// Remove the database directory
		mockState.EXPECT().OS().Return(mockOS)
		mockOS.EXPECT().GlobalDatabaseDir().Return("/var/lib/database/global")
		mockFileSystem.EXPECT().RemoveAll("/var/lib/database/global").Return(nil)

		// Initialize gateway
		mockGateway.EXPECT().Init(certInfo).Return(nil)
		mockGateway.EXPECT().Raft().Return(mockRaftInstance)
		mockRaftInstance.EXPECT().MembershipChanger().Return(mockRaftChanger)
		mockRaftChanger.EXPECT().Join(raft.ServerID("1"), raft.ServerAddress(""), 5*time.Second).Return(nil)

		// Cluster exit
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().ExitExclusive(gomock.Any()).Return(nil)

		join := membership.NewJoin(mockState, mockGateway, node.ConfigSchema,
			membership.WithConfigForJoin(mockConfigProvider),
			membership.WithFileSystemForJoin(mockFileSystem),
		)
		err = join.Run(certInfo, "node1", newRaftNodes)
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		if nodeMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
		if clusterMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
	})
}

func TestJoinExitExclusive(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, _ *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)

		mockTx := mocks.NewMockTx(ctrl)
		mockResult := mocks.NewMockResult(ctrl)
		mockQuery := mocks.NewMockQuery(ctrl)
		mockConfigProvider := mocks.NewMockNodeConfigProvider(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)

		nodeInfo := []db.NodeInfo{
			{
				ID:            int64(1),
				Name:          "node1",
				Address:       "[::]:8443",
				Schema:        1,
				APIExtensions: 1,
				Heartbeat:     time.Now(),
			},
		}
		nodeInfoMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfo)

		operations := []db.Operation{
			{
				UUID: "0feae487-148d-400f-b10d-d6d4cf7ad10a",
				Type: db.OperationType("lock"),
			},
		}

		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any(), 1).Return(nil)
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().NodeID(int64(1))
		mockQuery.EXPECT().UpsertObject(mockTx, gomock.Any(), []string{
			"uuid", "node_id", "type",
		}, []interface{}{
			operations[0].UUID, int64(1), operations[0].Type,
		}).Return(int64(1), nil)
		mockTx.EXPECT().Exec(gomock.Any(), 0, int64(1)).Return(mockResult, nil)
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

		join := membership.NewJoin(mockState, mockGateway, node.ConfigSchema,
			membership.WithConfigForJoin(mockConfigProvider),
			membership.WithFileSystemForJoin(mockFileSystem),
		)
		err := membership.JoinExitExclusive(join, "[::]:8443", operations)(clusterTx)
		if err != nil {
			t.Errorf("expected err to be nil")
		}
	})
}

func TestJoinExitExclusiveWithOperationAddFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, _ *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)

		mockTx := mocks.NewMockTx(ctrl)
		mockQuery := mocks.NewMockQuery(ctrl)
		mockConfigProvider := mocks.NewMockNodeConfigProvider(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)

		nodeInfo := []db.NodeInfo{
			{
				ID:            int64(1),
				Name:          "node1",
				Address:       "[::]:8443",
				Schema:        1,
				APIExtensions: 1,
				Heartbeat:     time.Now(),
			},
		}
		nodeInfoMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfo)

		operations := []db.Operation{
			{
				UUID: "0feae487-148d-400f-b10d-d6d4cf7ad10a",
				Type: db.OperationType("lock"),
			},
		}

		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any(), 1).Return(nil)
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().NodeID(int64(1))
		mockQuery.EXPECT().UpsertObject(mockTx, gomock.Any(), []string{
			"uuid", "node_id", "type",
		}, []interface{}{
			operations[0].UUID, int64(1), operations[0].Type,
		}).Return(int64(1), errors.New("bad"))

		join := membership.NewJoin(mockState, mockGateway, node.ConfigSchema,
			membership.WithConfigForJoin(mockConfigProvider),
			membership.WithFileSystemForJoin(mockFileSystem),
		)
		err := membership.JoinExitExclusive(join, "[::]:8443", operations)(clusterTx)
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}
