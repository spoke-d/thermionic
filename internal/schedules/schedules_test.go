package schedules_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/pborman/uuid"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/schedules"
	"github.com/spoke-d/thermionic/internal/schedules/mocks"
	"github.com/go-kit/kit/log"
	"github.com/golang/mock/gomock"
)

func TestScheduleAdd(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockClock := mocks.NewMockClock(ctrl)
	mockDaemon := mocks.NewMockDaemon(ctrl)
	mockCluster := mocks.NewMockCluster(ctrl)

	mockTx := mocks.NewMockTx(ctrl2)
	mockQuery := mocks.NewMockQuery(ctrl2)

	nodeID := int64(0)
	clusterTx := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)

	uuid := uuid.NewRandom().String()
	query := "SELECT * FROM nodes"
	timestamp := time.Now().UTC()

	mockDaemon.EXPECT().Cluster().Return(mockCluster)
	mockCluster.EXPECT().Transaction(ClusterTransactionMatcher(clusterTx)).Return(nil)
	mockQuery.EXPECT().UpsertObject(
		mockTx,
		"tasks",
		[]string{
			"uuid",
			"node_id",
			"query",
			"schedule",
			"result",
			"status",
		},
		[]interface{}{
			uuid,
			nodeID,
			query,
			timestamp.Unix(),
			"",
			schedules.Pending.Raw(),
		},
	)

	scheduler := schedules.New(
		mockDaemon,
		schedules.WithClock(mockClock),
		schedules.WithLogger(log.NewNopLogger()),
	)

	tsk := schedules.NewTask(uuid, query, timestamp)
	if err := scheduler.Add(tsk); err != nil {
		t.Errorf("expected err to be nil %v", err)
	}
}

func TestScheduleRemove(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockClock := mocks.NewMockClock(ctrl)
	mockDaemon := mocks.NewMockDaemon(ctrl)
	mockCluster := mocks.NewMockCluster(ctrl)

	mockTx := mocks.NewMockTx(ctrl2)
	mockQuery := mocks.NewMockQuery(ctrl2)
	mockResult := mocks.NewMockResult(ctrl2)

	nodeID := int64(0)
	clusterTx := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)

	uuid := uuid.NewRandom().String()
	input := []db.Task{
		{
			ID:          0,
			UUID:        uuid,
			NodeAddress: "0.0.0.0",
			Query:       "SELECT * FROM nodes;",
			Schedule:    time.Now().UTC().Unix(),
			Status:      0,
		},
	}

	mockDaemon.EXPECT().Cluster().Return(mockCluster)
	mockCluster.EXPECT().Transaction(ClusterTransactionMatcher(clusterTx)).Return(nil)
	mockQuery.EXPECT().SelectObjects(
		mockTx,
		TaskDestSelectObjectsMatcher(input),
		"SELECT tasks.id, uuid, nodes.address, query, schedule, result, status FROM tasks JOIN nodes ON nodes.id = node_id WHERE uuid=? ORDER BY tasks.id",
		uuid,
	).Return(nil)
	mockDaemon.EXPECT().Cluster().Return(mockCluster)
	mockCluster.EXPECT().Transaction(ClusterTransactionMatcher(clusterTx)).Return(nil)
	mockTx.EXPECT().Exec("DELETE FROM tasks WHERE uuid=?", uuid).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

	scheduler := schedules.New(
		mockDaemon,
		schedules.WithClock(mockClock),
		schedules.WithLogger(log.NewNopLogger()),
	)

	if err := scheduler.Remove(uuid); err != nil {
		t.Errorf("expected err to be nil %v", err)
	}
}

func TestScheduleWalk(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockClock := mocks.NewMockClock(ctrl)
	mockDaemon := mocks.NewMockDaemon(ctrl)
	mockCluster := mocks.NewMockCluster(ctrl)

	mockTx := mocks.NewMockTx(ctrl2)
	mockQuery := mocks.NewMockQuery(ctrl2)

	nodeID := int64(0)
	clusterTx := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)

	uuid := uuid.NewRandom().String()
	query := "SELECT * FROM nodes;"
	timestamp := time.Now().UTC().Unix()
	input := []db.Task{
		{
			ID:          0,
			UUID:        uuid,
			NodeAddress: "0.0.0.0",
			Query:       query,
			Schedule:    timestamp,
			Status:      0,
		},
	}

	mockDaemon.EXPECT().Cluster().Return(mockCluster)
	mockCluster.EXPECT().Transaction(ClusterTransactionMatcher(clusterTx)).Return(nil)
	mockQuery.EXPECT().SelectObjects(
		mockTx,
		TaskDestSelectObjectsMatcher(input),
		"SELECT tasks.id, uuid, nodes.address, query, schedule, result, status FROM tasks JOIN nodes ON nodes.id = node_id WHERE node_id=? ORDER BY tasks.id",
		int64(0),
	).Return(nil)

	scheduler := schedules.New(
		mockDaemon,
		schedules.WithClock(mockClock),
		schedules.WithLogger(log.NewNopLogger()),
	)

	var got []schedules.Tsk
	if err := scheduler.Walk(func(tsk schedules.Tsk) error {
		got = append(got, tsk)
		return nil
	}); err != nil {
		t.Errorf("expected err to be nil %v", err)
	}

	want := []schedules.Tsk{
		{
			ID:         uuid,
			Query:      query,
			Schedule:   time.Unix(timestamp, 0),
			Status:     schedules.Pending.String(),
			StatusCode: schedules.Pending,
			Result:     "",
			URL:        fmt.Sprintf("/schedules/%s", uuid),
		},
	}
	if expected, actual := want, got; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}
