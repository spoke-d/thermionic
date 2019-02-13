package db_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/mocks"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
)

func TestClusterTxTasksUUIDs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	nodeID := int64(0)
	input := []string{"15c0c9b1-eda7-4436-a8c5-82d2561a2a28"}

	mockQuery.EXPECT().SelectStrings(mockTx, "SELECT uuid FROM tasks WHERE node_id=?", nodeID).Return(input, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	nodeAddress, err := cluster.TasksUUIDs()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := input, nodeAddress; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestClusterTxTaskByUUID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.Task{
		{
			ID:          0,
			UUID:        "15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
			NodeAddress: "0.0.0.0",
			Query:       "SELECT * FROM nodes;",
			Schedule:    time.Now().UTC().Unix(),
			Status:      0,
		},
	}

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		TaskDestSelectObjectsMatcher(input),
		"SELECT tasks.id, uuid, nodes.address, query, schedule, result, status FROM tasks JOIN nodes ON nodes.id = node_id WHERE uuid=? ORDER BY tasks.id",
		"15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	operation, err := cluster.TaskByUUID("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := input[0], operation; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxTaskByUUIDWithNoTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.Task{}

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		TaskDestSelectObjectsMatcher(input),
		"SELECT tasks.id, uuid, nodes.address, query, schedule, result, status FROM tasks JOIN nodes ON nodes.id = node_id WHERE uuid=? ORDER BY tasks.id",
		"15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.TaskByUUID("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxTaskByUUIDWithMoreTasks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.Task{
		{
			ID:          0,
			UUID:        "15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
			NodeAddress: "0.0.0.0",
			Query:       "SELECT * FROM nodes;",
			Schedule:    time.Now().UTC().Unix(),
			Result:      "{}",
		},
		{
			ID:          0,
			UUID:        "15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
			NodeAddress: "0.0.0.0",
			Query:       "SELECT * FROM nodes;",
			Schedule:    time.Now().UTC().Unix(),
			Result:      "{}",
		},
	}

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		TaskDestSelectObjectsMatcher(input),
		"SELECT tasks.id, uuid, nodes.address, query, schedule, result, status FROM tasks JOIN nodes ON nodes.id = node_id WHERE uuid=? ORDER BY tasks.id",
		"15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.TaskByUUID("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxTaskAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	timestamp := time.Now().Unix()
	mockQuery.EXPECT().UpsertObject(mockTx, "tasks", []string{
		"uuid",
		"node_id",
		"query",
		"schedule",
		"result",
		"status",
	}, []interface{}{
		"15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
		nodeID,
		"SELECT * FROM nodes;",
		timestamp,
		"",
		0,
	}).Return(int64(10), nil)
	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	id, err := cluster.TaskAdd("15c0c9b1-eda7-4436-a8c5-82d2561a2a28", "SELECT * FROM nodes;", timestamp, 0)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := int64(10), id; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxTaskRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM tasks WHERE uuid=?", "15c0c9b1-eda7-4436-a8c5-82d2561a2a28").Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.TaskRemove("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterTxTaskRemoveWithRowsAffectedFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM tasks WHERE uuid=?", "15c0c9b1-eda7-4436-a8c5-82d2561a2a28").Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), errors.New("bad"))

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.TaskRemove("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxTaskRemoveWithMultipleRowsAffected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM tasks WHERE uuid=?", "15c0c9b1-eda7-4436-a8c5-82d2561a2a28").Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(2), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.TaskRemove("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}
