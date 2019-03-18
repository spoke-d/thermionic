package db_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/pborman/uuid"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/mocks"
)

func TestClusterTxServiceNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.ServiceNodeInfo{
		{
			ID:            0,
			Name:          "node1",
			Address:       "0.0.0.0",
			DaemonAddress: "0.0.0.1",
			DaemonNonce:   uuid.NewRandom().String(),
			Heartbeat:     time.Now(),
		},
	}

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		ServiceNodeInfoDestSelectObjectsMatcher(input),
		"SELECT id, name, address, daemon_address, daemon_nonce, heartbeat FROM services ORDER BY id",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	nodeInfo, err := cluster.ServiceNodes()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := input, nodeInfo; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxServiceNodesWithSelectObjectsFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.ServiceNodeInfo{
		{
			ID:            0,
			Name:          "node1",
			Address:       "0.0.0.0",
			DaemonAddress: "0.0.0.1",
			DaemonNonce:   uuid.NewRandom().String(),
			Heartbeat:     time.Now(),
		},
	}

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		ServiceNodeInfoDestSelectObjectsMatcher(input),
		"SELECT id, name, address, daemon_address, daemon_nonce, heartbeat FROM services ORDER BY id",
	).Return(errors.New("bad"))

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.ServiceNodes()
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxServiceNodeOfflineThreshold(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockQuery.EXPECT().SelectStrings(mockTx, "SELECT value FROM config WHERE key='discovery.offline_threshold'").Return([]string{"5"}, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	threshold, err := cluster.ServiceNodeOfflineThreshold()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := 5*time.Second, threshold; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxServiceNodeOfflineThresholdWithNoResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockQuery.EXPECT().SelectStrings(mockTx, "SELECT value FROM config WHERE key='discovery.offline_threshold'").Return([]string{}, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	threshold, err := cluster.ServiceNodeOfflineThreshold()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := db.DiscoveryDefaultOfflineThreshold*time.Second, threshold; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxServiceNodeOfflineThresholdWithInvalidNumber(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockQuery.EXPECT().SelectStrings(mockTx, "SELECT value FROM config WHERE key='discovery.offline_threshold'").Return([]string{"aaa"}, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.ServiceNodeOfflineThreshold()
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxServiceAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)
	nonce := uuid.NewRandom().String()

	mockQuery.EXPECT().UpsertObject(mockTx, "services", []string{
		"name",
		"address",
		"daemon_address",
		"daemon_nonce",
	}, []interface{}{
		"name",
		"0.0.0.0",
		"0.0.0.1",
		nonce,
	}).Return(int64(10), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	id, err := cluster.ServiceAdd("name", "0.0.0.0", "0.0.0.1", nonce)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := int64(10), id; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxServiceUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)
	nonce := uuid.NewRandom().String()

	mockTx.EXPECT().Exec("UPDATE services SET name=?, address=?, daemon_address=?, daemon_nonce=? WHERE id=?", "name", "0.0.0.0", "0.0.0.1", nonce, nodeID).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.ServiceUpdate(nodeID, "name", "0.0.0.0", "0.0.0.1", nonce)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterTxServiceUpdateWithExecFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)
	nonce := uuid.NewRandom().String()

	mockTx.EXPECT().Exec("UPDATE services SET name=?, address=?, daemon_address=?, daemon_nonce=? WHERE id=?", "name", "0.0.0.0", "0.0.0.1", nonce, nodeID).Return(mockResult, errors.New("bad"))

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.ServiceUpdate(nodeID, "name", "0.0.0.0", "0.0.0.1", nonce)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxServiceUpdateWithRowAffectedFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)
	nonce := uuid.NewRandom().String()

	mockTx.EXPECT().Exec("UPDATE services SET name=?, address=?, daemon_address=?, daemon_nonce=? WHERE id=?", "name", "0.0.0.0", "0.0.0.1", nonce, nodeID).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), errors.New("bad"))

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.ServiceUpdate(nodeID, "name", "0.0.0.0", "0.0.0.1", nonce)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxServiceUpdateWithTooManyRowAffected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)
	nonce := uuid.NewRandom().String()

	mockTx.EXPECT().Exec("UPDATE services SET name=?, address=?, daemon_address=?, daemon_nonce=? WHERE id=?", "name", "0.0.0.0", "0.0.0.1", nonce, nodeID).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(2), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.ServiceUpdate(nodeID, "name", "0.0.0.0", "0.0.0.1", nonce)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxServiceRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM services WHERE id=?", int64(1)).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.ServiceRemove(int64(1))
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterTxServiceRemoveWithExecFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM services WHERE id=?", int64(1)).Return(mockResult, errors.New("bad"))

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.ServiceRemove(int64(1))
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxServiceRemoveWithMoreRowsAffected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM services WHERE id=?", int64(1)).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(2), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.ServiceRemove(int64(1))
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}
