package services_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/cluster/services"
	"github.com/spoke-d/thermionic/internal/cluster/services/mocks"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
)

func TestList(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockClock := mocks.NewMockClock(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(0), mockQuery)

		nonce := uuid.NewRandom().String()
		nodeInfo := []db.ServiceNodeInfo{
			{
				ID:            0,
				Name:          "node1",
				Address:       "10.0.0.0",
				DaemonAddress: "10.0.0.1",
				DaemonNonce:   nonce,
				Heartbeat:     time.Now(),
			},
		}

		clusterMatcher := ClusterTransactionMatcher(clusterTx)
		nodeInfoMatcher := ServiceNodeInfoDestSelectObjectsMatcher(nodeInfo)

		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any()).Return(nil)
		mockQuery.EXPECT().SelectStrings(mockTx, gomock.Any()).Return([]string{"5"}, nil)

		mockClock.EXPECT().Now().Return(time.Now()).Times(2)

		list := services.NewList(
			mockState,
			services.WithClockForList(mockClock),
		)
		members, err := list.Run()
		if err != nil {
			t.Errorf("expected err to be nil")
		}

		wanted := []services.ServiceMember{
			{
				ServerName:    "node1",
				ServerAddress: "10.0.0.0",
				DaemonAddress: "10.0.0.1",
				DaemonNonce:   nonce,
				Status:        "online",
				Message:       "fully operational",
			},
		}

		if expected, actual := wanted, members; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}

func TestListWithNoNodes(t *testing.T) {
	setup(t, func(t *testing.T, ctrl, ctrl2 *gomock.Controller) {
		mockState := mocks.NewMockState(ctrl)
		mockClock := mocks.NewMockClock(ctrl)
		mockCluster := mocks.NewMockCluster(ctrl)

		mockTx := mocks.NewMockTx(ctrl2)
		mockQuery := mocks.NewMockQuery(ctrl2)

		clusterTx := db.NewClusterTxWithQuery(mockTx, int64(0), mockQuery)

		nodeInfo := []db.ServiceNodeInfo{}

		clusterMatcher := ClusterTransactionMatcher(clusterTx)
		nodeInfoMatcher := ServiceNodeInfoDestSelectObjectsMatcher(nodeInfo)

		mockState.EXPECT().Cluster().Return(mockCluster)
		mockCluster.EXPECT().Transaction(clusterMatcher).Return(nil)
		mockQuery.EXPECT().SelectObjects(mockTx, nodeInfoMatcher, gomock.Any()).Return(nil)
		mockQuery.EXPECT().SelectStrings(mockTx, gomock.Any()).Return([]string{"5"}, nil)

		list := services.NewList(
			mockState,
			services.WithClockForList(mockClock),
		)
		members, err := list.Run()
		if err != nil {
			t.Errorf("expected err to be nil")
		}

		wanted := []services.ServiceMember{}

		if expected, actual := wanted, members; !reflect.DeepEqual(expected, actual) {
			t.Errorf("expected: %v, actual: %v", expected, actual)
		}
	})
}
