package membership_test

import (
	"testing"
	"time"

	rafthttp "github.com/CanonicalLtd/raft-http"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cert"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/membership/mocks"
	"github.com/spoke-d/thermionic/internal/db"

	"github.com/golang/mock/gomock"
)

func TestLeave(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockDialer := mocks.NewMockDialerProvider(ctrl)
		mockMembership := mocks.NewMockMembership(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		raftNodes := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.1",
			},
		}
		raftNodeMatcher := NodeDestSelectObjectsMatcher(raftNodes)
		nodeInfoName := []db.NodeInfo{
			{
				ID:            int64(1),
				Name:          "node1",
				Address:       "10.0.0.1",
				APIExtensions: 1,
				Schema:        1,
				Heartbeat:     time.Now(),
			},
		}
		nodeInfoNodes := []db.NodeInfo{
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
		nodeInfoNameMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfoName)
		nodeInfoNodesMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfoNodes)
		certInfo := &cert.Info{}
		dialer := rafthttp.NewDialTCP()

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoNameMatcher, gomock.Any(), 0).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoNodesMatcher, gomock.Any(), 0).Return(nil)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, raftNodeMatcher, gomock.Any()).Return(nil)

		// Raft actions
		mockGateway.EXPECT().Cert().Return(certInfo)
		mockDialer.EXPECT().Dial(certInfo).Return(dialer, nil)
		mockMembership.EXPECT().Change(DialMatcher(dialer), raft.ServerID("1"), "10.0.0.1", "10.0.0.1", 5*time.Second).Return(nil)

		leave := membership.NewLeave(
			mockState, mockGateway,
			membership.WithMembershipForLeave(mockMembership),
			membership.WithDialerProviderForLeave(mockDialer),
		)
		address, err := leave.Run("name", false)
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		if clusterMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
		if nodeMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
		if expected, actual := "10.0.0.1", address; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestLeaveWithNoNodesFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockDialer := mocks.NewMockDialerProvider(ctrl)
		mockMembership := mocks.NewMockMembership(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		nodeInfoName := []db.NodeInfo{}
		nodeInfoNameMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfoName)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(errors.New("bad"))
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoNameMatcher, gomock.Any(), 0).Return(nil)

		leave := membership.NewLeave(
			mockState, mockGateway,
			membership.WithMembershipForLeave(mockMembership),
			membership.WithDialerProviderForLeave(mockDialer),
		)
		_, err := leave.Run("name", false)
		if err == nil {
			t.Errorf("expected err to be nil")
		}
		if clusterMatcher.Err() == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

func TestLeaveWithNoClusterNodesFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockDialer := mocks.NewMockDialerProvider(ctrl)
		mockMembership := mocks.NewMockMembership(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		nodeInfoName := []db.NodeInfo{
			{
				ID:            int64(1),
				Name:          "node1",
				Address:       "10.0.0.1",
				APIExtensions: 1,
				Schema:        1,
				Heartbeat:     time.Now(),
			},
		}
		nodeInfoNodes := []db.NodeInfo{
			{
				ID:            int64(1),
				Name:          "node1",
				Address:       "10.0.0.1",
				APIExtensions: 1,
				Schema:        1,
				Heartbeat:     time.Now(),
			},
		}
		nodeInfoNameMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfoName)
		nodeInfoNodesMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfoNodes)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(errors.New("bad"))
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoNameMatcher, gomock.Any(), 0).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoNodesMatcher, gomock.Any(), 0).Return(nil)

		leave := membership.NewLeave(
			mockState, mockGateway,
			membership.WithMembershipForLeave(mockMembership),
			membership.WithDialerProviderForLeave(mockDialer),
		)
		_, err := leave.Run("name", false)
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
		if clusterMatcher.Err() == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

func TestLeaveWithRaftNodesFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockDialer := mocks.NewMockDialerProvider(ctrl)
		mockMembership := mocks.NewMockMembership(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		raftNodes := []db.RaftNode{}
		raftNodeMatcher := NodeDestSelectObjectsMatcher(raftNodes)
		nodeInfoName := []db.NodeInfo{
			{
				ID:            int64(1),
				Name:          "node1",
				Address:       "10.0.0.1",
				APIExtensions: 1,
				Schema:        1,
				Heartbeat:     time.Now(),
			},
		}
		nodeInfoNodes := []db.NodeInfo{
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
		nodeInfoNameMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfoName)
		nodeInfoNodesMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfoNodes)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoNameMatcher, gomock.Any(), 0).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoNodesMatcher, gomock.Any(), 0).Return(nil)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(errors.New("bad"))
		mockQuery.EXPECT().SelectObjects(mockTx, raftNodeMatcher, gomock.Any()).Return(errors.New("bad"))

		leave := membership.NewLeave(
			mockState, mockGateway,
			membership.WithMembershipForLeave(mockMembership),
			membership.WithDialerProviderForLeave(mockDialer),
		)
		_, err := leave.Run("name", false)
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
		if clusterMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
		if nodeMatcher.Err() == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

func TestLeaveWithNoMatchingAddress(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockNode := mocks.NewMockNode(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockDialer := mocks.NewMockDialerProvider(ctrl)
		mockMembership := mocks.NewMockMembership(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)
		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		raftNodes := []db.RaftNode{
			{
				ID:      int64(6),
				Address: "10.0.0.10",
			},
		}
		raftNodeMatcher := NodeDestSelectObjectsMatcher(raftNodes)
		nodeInfoName := []db.NodeInfo{
			{
				ID:            int64(1),
				Name:          "node1",
				Address:       "10.0.0.1",
				APIExtensions: 1,
				Schema:        1,
				Heartbeat:     time.Now(),
			},
		}
		nodeInfoNodes := []db.NodeInfo{
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
		nodeInfoNameMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfoName)
		nodeInfoNodesMatcher := NodeInfoDestSelectObjectsMatcher(nodeInfoNodes)

		// Cluster actions
		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoNameMatcher, gomock.Any(), 0).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoNodesMatcher, gomock.Any(), 0).Return(nil)

		// Node actions
		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, raftNodeMatcher, gomock.Any()).Return(nil)

		leave := membership.NewLeave(
			mockState, mockGateway,
			membership.WithMembershipForLeave(mockMembership),
			membership.WithDialerProviderForLeave(mockDialer),
		)
		address, err := leave.Run("name", false)
		if err != nil {
			t.Errorf("expected err not to be nil")
		}
		if clusterMatcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
		if nodeMatcher.Err() != nil {
			t.Errorf("expected err not to be nil")
		}
		if expected, actual := "10.0.0.1", address; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}
