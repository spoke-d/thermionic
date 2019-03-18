package membership_test

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/membership/mocks"
	"github.com/spoke-d/thermionic/internal/config"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/node"
	libtesting "github.com/spoke-d/thermionic/internal/testing"
)

func TestBootstrap(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockResult := mocks.NewMockResult(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockConfigProvider := mocks.NewMockNodeConfigProvider(ctrl2)
		mockOS := mocks.NewMockOS(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		certInfo := libtesting.KeyPair()
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
		raftNodes := []db.RaftNode{}
		raftNodeMatcher := NodeDestSelectObjectsMatcher(raftNodes)
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

		mockState.EXPECT().OS().Return(mockOS)
		mockOS.EXPECT().VarDir().Return("/var/lib")
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.crt").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.key").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.ca").Return(false)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
		mockConfigProvider.EXPECT().ConfigLoad(nodeTx, node.ConfigSchema).Return(nodeConfig, nil)
		mockQuery.EXPECT().SelectObjects(mockTx, raftNodeMatcher, gomock.Any()).Return(nil)
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id", "address",
		}, []interface{}{
			int64(1), "[::]:8443",
		}).Return(int64(1), nil)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any(), 0).Return(nil)
		mockTx.EXPECT().Exec(gomock.Any(), "foo", "[::]:8443", int64(1)).Return(mockResult, nil)
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

		// Cluster leadership actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().EnterExclusive().Return(nil)
		mockGateway.EXPECT().Shutdown().Return(nil)
		mockGateway.EXPECT().Init(certInfo).Return(nil)
		mockGateway.EXPECT().WaitLeadership().Return(nil)

		// Verify
		mockFileSystem.EXPECT().Exists("/var/lib/server.ca").Return(true)
		mockFileSystem.EXPECT().Symlink("server.crt", "/var/lib/cluster.crt")
		mockFileSystem.EXPECT().Symlink("server.key", "/var/lib/cluster.key")
		mockFileSystem.EXPECT().Symlink("server.ca", "/var/lib/cluster.ca")

		// Exit
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().ExitExclusive(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any(), 0).Return(nil)

		bootstrap := membership.NewBootstrap(mockState, mockGateway, certInfo, node.ConfigSchema,
			membership.WithConfigForBootstrap(mockConfigProvider),
			membership.WithFileSystemForBootstrap(mockFileSystem),
		)
		err = bootstrap.Run("foo")
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

func TestBootstrapWithConfigFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)
		mockNode := mocks.NewMockNode(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockConfigProvider := mocks.NewMockNodeConfigProvider(ctrl2)
		mockOS := mocks.NewMockOS(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)

		certInfo := libtesting.KeyPair()
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

		mockState.EXPECT().OS().Return(mockOS)
		mockOS.EXPECT().VarDir().Return("/var/lib")
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.crt").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.key").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.ca").Return(false)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(errors.New("bad"))
		mockConfigProvider.EXPECT().ConfigLoad(nodeTx, node.ConfigSchema).Return(nodeConfig, errors.New("bad"))

		bootstrap := membership.NewBootstrap(mockState, mockGateway, certInfo, node.ConfigSchema,
			membership.WithConfigForBootstrap(mockConfigProvider),
			membership.WithFileSystemForBootstrap(mockFileSystem),
		)
		err = bootstrap.Run("foo")
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
		if nodeMatcher.Err() == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

func TestBootstrapWithNoConfigHTTPSAddress(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {

		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)
		mockNode := mocks.NewMockNode(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockConfigProvider := mocks.NewMockNodeConfigProvider(ctrl2)
		mockOS := mocks.NewMockOS(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)

		certInfo := libtesting.KeyPair()
		nodeConfig := &node.Config{}

		mockState.EXPECT().OS().Return(mockOS)
		mockOS.EXPECT().VarDir().Return("/var/lib")
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.crt").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.key").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.ca").Return(false)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(errors.New("bad"))
		mockConfigProvider.EXPECT().ConfigLoad(nodeTx, node.ConfigSchema).Return(nodeConfig, nil)

		bootstrap := membership.NewBootstrap(mockState, mockGateway, certInfo, node.ConfigSchema,
			membership.WithConfigForBootstrap(mockConfigProvider),
			membership.WithFileSystemForBootstrap(mockFileSystem),
		)
		err := bootstrap.Run("foo")
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
		if nodeMatcher.Err() == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

func TestBootstrapWithRaftNodeFirstFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)
		mockNode := mocks.NewMockNode(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockConfigProvider := mocks.NewMockNodeConfigProvider(ctrl2)
		mockOS := mocks.NewMockOS(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)

		certInfo := libtesting.KeyPair()
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
		raftNodes := []db.RaftNode{}
		raftNodeMatcher := NodeDestSelectObjectsMatcher(raftNodes)

		mockState.EXPECT().OS().Return(mockOS)
		mockOS.EXPECT().VarDir().Return("/var/lib")
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.crt").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.key").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.ca").Return(false)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(errors.New("bad"))
		mockConfigProvider.EXPECT().ConfigLoad(nodeTx, node.ConfigSchema).Return(nodeConfig, nil)
		mockQuery.EXPECT().SelectObjects(mockTx, raftNodeMatcher, gomock.Any()).Return(nil)
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id", "address",
		}, []interface{}{
			int64(1), "[::]:8443",
		}).Return(int64(1), errors.New("bad"))

		bootstrap := membership.NewBootstrap(mockState, mockGateway, certInfo, node.ConfigSchema,
			membership.WithConfigForBootstrap(mockConfigProvider),
			membership.WithFileSystemForBootstrap(mockFileSystem),
		)
		err = bootstrap.Run("foo")
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
		if nodeMatcher.Err() == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

func TestBootstrapWithShutdownFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockResult := mocks.NewMockResult(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockConfigProvider := mocks.NewMockNodeConfigProvider(ctrl2)
		mockOS := mocks.NewMockOS(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		certInfo := libtesting.KeyPair()
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
		raftNodes := []db.RaftNode{}
		raftNodeMatcher := NodeDestSelectObjectsMatcher(raftNodes)
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

		mockState.EXPECT().OS().Return(mockOS)
		mockOS.EXPECT().VarDir().Return("/var/lib")
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.crt").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.key").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.ca").Return(false)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
		mockConfigProvider.EXPECT().ConfigLoad(nodeTx, node.ConfigSchema).Return(nodeConfig, nil)
		mockQuery.EXPECT().SelectObjects(mockTx, raftNodeMatcher, gomock.Any()).Return(nil)
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id", "address",
		}, []interface{}{
			int64(1), "[::]:8443",
		}).Return(int64(1), nil)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any(), 0).Return(nil)
		mockTx.EXPECT().Exec(gomock.Any(), "foo", "[::]:8443", int64(1)).Return(mockResult, nil)
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

		// Cluster leadership actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().EnterExclusive().Return(nil)
		mockGateway.EXPECT().Shutdown().Return(errors.New("bad"))

		bootstrap := membership.NewBootstrap(mockState, mockGateway, certInfo, node.ConfigSchema,
			membership.WithConfigForBootstrap(mockConfigProvider),
			membership.WithFileSystemForBootstrap(mockFileSystem),
		)
		err = bootstrap.Run("foo")
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
		if nodeMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
		if clusterMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
	})
}

func TestBootstrapWithWaitLeadershipFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockFileSystem := mocks.NewMockFileSystem(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockResult := mocks.NewMockResult(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockConfigProvider := mocks.NewMockNodeConfigProvider(ctrl2)
		mockOS := mocks.NewMockOS(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		certInfo := libtesting.KeyPair()
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
		raftNodes := []db.RaftNode{}
		raftNodeMatcher := NodeDestSelectObjectsMatcher(raftNodes)
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

		mockState.EXPECT().OS().Return(mockOS)
		mockOS.EXPECT().VarDir().Return("/var/lib")
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.crt").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.key").Return(false)
		mockFileSystem.EXPECT().Exists("/var/lib/cluster.ca").Return(false)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
		mockConfigProvider.EXPECT().ConfigLoad(nodeTx, node.ConfigSchema).Return(nodeConfig, nil)
		mockQuery.EXPECT().SelectObjects(mockTx, raftNodeMatcher, gomock.Any()).Return(nil)
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id", "address",
		}, []interface{}{
			int64(1), "[::]:8443",
		}).Return(int64(1), nil)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any(), 0).Return(nil)
		mockTx.EXPECT().Exec(gomock.Any(), "foo", "[::]:8443", int64(1)).Return(mockResult, nil)
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

		// Cluster leadership actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().EnterExclusive().Return(nil)
		mockGateway.EXPECT().Shutdown().Return(nil)
		mockGateway.EXPECT().Init(certInfo).Return(nil)
		mockGateway.EXPECT().WaitLeadership().Return(errors.New("bad"))

		bootstrap := membership.NewBootstrap(mockState, mockGateway, certInfo, node.ConfigSchema,
			membership.WithConfigForBootstrap(mockConfigProvider),
			membership.WithFileSystemForBootstrap(mockFileSystem),
		)
		err = bootstrap.Run("foo")
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
		if nodeMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
		if clusterMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
	})
}
