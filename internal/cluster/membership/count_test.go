package membership_test

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/cluster/membership"
	"github.com/spoke-d/thermionic/internal/cluster/membership/mocks"
	"github.com/spoke-d/thermionic/internal/db"
)

func TestCount(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		matcher := ClusterTransactionMatcher(clusterTx)

		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(matcher).Return(nil)
		mockQuery.EXPECT().Count(mockTx, "nodes", "").Return(2, nil)

		procedure := membership.NewCount(mockState)
		count, err := procedure.Run()
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		if matcher.Err() != nil {
			t.Errorf("expected err to be nil")
		}
		if expected, actual := 2, count; expected != actual {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestCountWithNodesCountFailure(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(1), mockQuery)
		matcher := ClusterTransactionMatcher(clusterTx)

		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(matcher).Return(nil)
		mockQuery.EXPECT().Count(mockTx, "nodes", "").Return(2, errors.New("bad"))

		procedure := membership.NewCount(mockState)
		_, err := procedure.Run()
		if err != nil {
			t.Errorf("expected err to be nil")
		}
		if matcher.Err() == nil {
			t.Errorf("expected err not to be nil")
		}
	})
}
