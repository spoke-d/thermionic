package membership_test

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/membership/mocks"
	"github.com/spoke-d/thermionic/internal/db"
)

func TestPurge(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)
		mockResult := mocks.NewMockResult(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		matcher := ClusterTransactionMatcher(clusterTx)

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

		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(matcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, NodeInfoDestSelectObjectsMatcher(info), gomock.Any(), 0, "node1").Return(nil)
		mockTx.EXPECT().Exec(gomock.Any(), int64(0)).Return(mockResult, nil)
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

		procedure := membership.NewPurge(mockState)
		err := procedure.Run("node1")
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		if matcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
	})
}

func TestPurgeWithNodesPurgeFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		matcher := ClusterTransactionMatcher(clusterTx)

		info := []db.NodeInfo{}

		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(matcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, NodeInfoDestSelectObjectsMatcher(info), gomock.Any(), 0, "node1").Return(errors.New("bad"))

		procedure := membership.NewPurge(mockState)
		err := procedure.Run("node1")
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		if matcher.Err() == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}
