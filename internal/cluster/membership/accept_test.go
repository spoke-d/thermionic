package membership_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/membership/mocks"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/golang/mock/gomock"
)

func TestAccept(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)
		mockNode := mocks.NewMockNode(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockResult := mocks.NewMockResult(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		clusterMatcher := ClusterTransactionMatcher(clusterTx)

		info := []db.NodeInfo{
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

		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)

		mockQuery.EXPECT().SelectObjects(mockTx, NodeInfoDestSelectObjectsMatcher(info), gomock.Any(), 0).Return(nil)
		mockQuery.EXPECT().UpsertObject(mockTx, "nodes",
			[]string{"name", "address", "schema", "api_extensions"},
			[]interface{}{"node2", "10.0.0.1", 1, 1},
		).Return(int64(2), nil)
		mockTx.EXPECT().Exec("UPDATE nodes SET pending=? WHERE id=?", 1, int64(2)).Return(mockResult, nil)
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

		nodes := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.0",
			},
		}

		nodeTx := db.NewNodeTxWithQuery(mockTx, mockQuery)
		nodeMatcher := NodeTransactionMatcher(nodeTx)

		mockGateway.EXPECT().RaftNodes().Return(nodes, nil)

		mockState.EXPECT().Node().Return(mockNode)
		mockNode.EXPECT().Transaction(nodeMatcher).Return(nil)

		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes",
			[]string{"address"},
			[]interface{}{"10.0.0.1"},
		).Return(int64(2), nil)

		procedure := membership.NewAccept(mockState, mockGateway)
		got, err := procedure.Run("node2", "10.0.0.1", 1, 1)
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		if expected, actual := []db.RaftNode{
			{
				ID:      int64(1),
				Address: "10.0.0.0",
			},
			{
				ID:      int64(2),
				Address: "10.0.0.1",
			},
		}, got; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestAcceptWithInvalidNameArgument(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, _ *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)

		procedure := membership.NewAccept(mockState, mockGateway)
		_, err := procedure.Run("", "10.0.0.1", 1, 1)
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

func TestAcceptWithInvalidAddressArgument(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, _ *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockGateway := mocks.NewMockGateway(ctrl)

		procedure := membership.NewAccept(mockState, mockGateway)
		_, err := procedure.Run("node1", "", 1, 1)
		if err == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}

// MembershipCheckClusterStateForAccept

func TestMembershipCheckClusterStateForAcceptWithOneLocalNode(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockTx := mocks.NewMockTx(ctrl)
		mockQuery := mocks.NewMockQuery(ctrl)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)

		info := []db.NodeInfo{
			{
				ID:            0,
				Name:          "node1",
				Address:       "0.0.0.0",
				Description:   "node 1",
				Schema:        1,
				APIExtensions: 1,
				Heartbeat:     time.Now(),
			},
		}

		mockQuery.EXPECT().SelectObjects(mockTx, NodeInfoDestSelectObjectsMatcher(info), gomock.Any(), 0).Return(nil)

		err := membership.MembershipCheckClusterStateForAccept(clusterTx, "node1", "0.0.0.0", 1, 1)
		if err == nil {
			t.Errorf("expected err to be nil")
		}
	})
}

func TestMembershipCheckClusterStateForAcceptFailures(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockTx := mocks.NewMockTx(ctrl)
		mockQuery := mocks.NewMockQuery(ctrl)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)

		info := []db.NodeInfo{
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

		testCases := []struct {
			desc    string
			name    string
			address string
			schema  int
			api     int
		}{
			{
				desc:    "same name",
				name:    "node1",
				address: "10.0.0.0",
				schema:  1,
				api:     1,
			},
			{
				desc:    "same address",
				name:    "node2",
				address: "10.0.0.0",
				schema:  1,
				api:     1,
			},
			{
				desc:    "different schema",
				name:    "node2",
				address: "10.0.0.1",
				schema:  2,
				api:     1,
			},
			{
				desc:    "different api",
				name:    "node2",
				address: "10.0.0.1",
				schema:  1,
				api:     2,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				mockQuery.EXPECT().SelectObjects(mockTx, NodeInfoDestSelectObjectsMatcher(info), gomock.Any(), 0).Return(nil)

				err := membership.MembershipCheckClusterStateForAccept(clusterTx, tc.name, tc.address, tc.schema, tc.api)
				if err == nil {
					t.Errorf("expected err to be nil")
				}
			})
		}
	})
}
