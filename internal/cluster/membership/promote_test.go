package membership_test

import (
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/membership/mocks"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

func TestPromote(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockRaftInstance := mocks.NewMockRaftInstance(ctrl)
		mockChanger := mocks.NewMockChanger(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockOS := mocks.NewMockOS(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		certInfo := &cert.Info{}
		raftNodes := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.1",
			},
		}
		nodeInfo := []db.NodeInfo{
			{
				ID:            int64(1),
				Name:          "node1",
				Address:       "10.0.0.1",
				APIExtensions: 1,
				Schema:        1,
				Heartbeat:     time.Now(),
			},
		}
		nodeInfoMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfo)

		mockGateway.EXPECT().IsDatabaseNode().Return(false)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectStrings(mockTx, gomock.Any(), int64(1)).Return([]string{"10.0.0.1"}, nil)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
		mockTx.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id", "address",
		}, []interface{}{
			int64(1), "10.0.0.1",
		}).Return(int64(0), nil)

		// Membership actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().EnterExclusive().Return(nil)
		mockState.EXPECT().OS().Return(mockOS)
		mockOS.EXPECT().GlobalDatabaseDir().Return("/var/lib/database/global")
		mockFileSystem.EXPECT().RemoveAll("/var/lib/database/global").Return(nil)
		mockGateway.EXPECT().Init(certInfo).Return(nil)
		mockGateway.EXPECT().Raft().Return(mockRaftInstance)
		mockRaftInstance.EXPECT().MembershipChanger().Return(mockChanger)
		mockChanger.EXPECT().Join(raft.ServerID("1"), raft.ServerAddress(""), 5*time.Second).Return(nil)
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().ExitExclusive(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any(), 0).Return(nil)

		promote := membership.NewPromote(
			mockState, mockGateway,
			membership.WithFileSystemForPromote(mockFileSystem),
		)
		err := promote.Run(certInfo, raftNodes)
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		if clusterMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
		if nodeMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
	})
}

func TestPromoteWhenAlreadyADatabaseNode(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, _ *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)

		certInfo := &cert.Info{}
		raftNodes := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.1",
			},
		}

		mockGateway.EXPECT().IsDatabaseNode().Return(true)

		promote := membership.NewPromote(
			mockState, mockGateway,
			membership.WithFileSystemForPromote(mockFileSystem),
		)
		err := promote.Run(certInfo, raftNodes)
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

func TestPromoteWithNoNodeAddress(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		certInfo := &cert.Info{}
		raftNodes := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.1",
			},
		}

		mockGateway.EXPECT().IsDatabaseNode().Return(false)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectStrings(mockTx, gomock.Any(), int64(1)).Return([]string{}, nil)

		promote := membership.NewPromote(
			mockState, mockGateway,
			membership.WithFileSystemForPromote(mockFileSystem),
		)
		err := promote.Run(certInfo, raftNodes)
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

func TestPromoteWithNodeAddressMissmatch(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		certInfo := &cert.Info{}
		raftNodes := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.1",
			},
		}

		mockGateway.EXPECT().IsDatabaseNode().Return(false)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectStrings(mockTx, gomock.Any(), int64(1)).Return([]string{"10.0.0.10"}, nil)

		promote := membership.NewPromote(
			mockState, mockGateway,
			membership.WithFileSystemForPromote(mockFileSystem),
		)
		err := promote.Run(certInfo, raftNodes)
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

func TestPromoteWithFileSystemFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockOS := mocks.NewMockOS(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		certInfo := &cert.Info{}
		raftNodes := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.1",
			},
		}

		mockGateway.EXPECT().IsDatabaseNode().Return(false)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectStrings(mockTx, gomock.Any(), int64(1)).Return([]string{"10.0.0.1"}, nil)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
		mockTx.EXPECT().Exec(gomock.Any()).Return(nil, nil)
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id", "address",
		}, []interface{}{
			int64(1), "10.0.0.1",
		}).Return(int64(0), nil)

		// Membership actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().EnterExclusive().Return(nil)
		mockState.EXPECT().OS().Return(mockOS)
		mockOS.EXPECT().GlobalDatabaseDir().Return("/var/lib/database/global")
		mockFileSystem.EXPECT().RemoveAll("/var/lib/database/global").Return(errors.New("bad"))

		promote := membership.NewPromote(
			mockState, mockGateway,
			membership.WithFileSystemForPromote(mockFileSystem),
		)
		err := promote.Run(certInfo, raftNodes)
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}
