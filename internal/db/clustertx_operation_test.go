package db_test

import (
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/mocks"
)

func TestClusterTxOperationsUUIDs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	nodeID := int64(0)
	input := []string{"15c0c9b1-eda7-4436-a8c5-82d2561a2a28"}

	mockQuery.EXPECT().SelectStrings(mockTx, "SELECT uuid FROM operations WHERE node_id=?", nodeID).Return(input, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	nodeAddress, err := cluster.OperationsUUIDs()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := input, nodeAddress; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestClusterTxOpertionByUUID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.Operation{
		{
			ID:          0,
			UUID:        "15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
			NodeAddress: "0.0.0.0",
			Type:        db.OperationType("lock"),
		},
	}

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		OperationDestSelectObjectsMatcher(input),
		"SELECT operations.id, uuid, nodes.address, type FROM operations JOIN nodes ON nodes.id = node_id WHERE uuid=? ORDER BY operations.id",
		"15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	operation, err := cluster.OperationByUUID("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := input[0], operation; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxOpertionByUUIDWithNoOperations(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.Operation{}

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		OperationDestSelectObjectsMatcher(input),
		"SELECT operations.id, uuid, nodes.address, type FROM operations JOIN nodes ON nodes.id = node_id WHERE uuid=? ORDER BY operations.id",
		"15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.OperationByUUID("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxOpertionByUUIDWithMoreOperations(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.Operation{
		{
			ID:          0,
			UUID:        "15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
			NodeAddress: "0.0.0.0",
			Type:        db.OperationType("lock"),
		},
		{
			ID:          0,
			UUID:        "15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
			NodeAddress: "0.0.0.0",
			Type:        db.OperationType("lock"),
		},
	}

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		OperationDestSelectObjectsMatcher(input),
		"SELECT operations.id, uuid, nodes.address, type FROM operations JOIN nodes ON nodes.id = node_id WHERE uuid=? ORDER BY operations.id",
		"15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.OperationByUUID("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxOperationAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockQuery.EXPECT().UpsertObject(mockTx, "operations", []string{
		"uuid",
		"node_id",
		"type",
	}, []interface{}{
		"15c0c9b1-eda7-4436-a8c5-82d2561a2a28",
		nodeID,
		db.OperationType("lock"),
	}).Return(int64(10), nil)
	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	id, err := cluster.OperationAdd("15c0c9b1-eda7-4436-a8c5-82d2561a2a28", db.OperationType("lock"))
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := int64(10), id; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxOperationRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM operations WHERE uuid=?", "15c0c9b1-eda7-4436-a8c5-82d2561a2a28").Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.OperationRemove("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterTxOperationRemoveWithRowsAffectedFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM operations WHERE uuid=?", "15c0c9b1-eda7-4436-a8c5-82d2561a2a28").Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), errors.New("bad"))

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.OperationRemove("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxOperationRemoveWithMultipleRowsAffected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM operations WHERE uuid=?", "15c0c9b1-eda7-4436-a8c5-82d2561a2a28").Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(2), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.OperationRemove("15c0c9b1-eda7-4436-a8c5-82d2561a2a28")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}
