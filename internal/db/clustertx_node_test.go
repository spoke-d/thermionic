package db_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/mocks"
)

func TestClusterTxNodeAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	nodeID := int64(0)
	input := "0.0.0.0"

	mockQuery.EXPECT().SelectStrings(mockTx, "SELECT address FROM nodes WHERE id=?", nodeID).Return([]string{input}, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	nodeAddress, err := cluster.NodeAddress()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := input, nodeAddress; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestClusterTxNodeAddressWithNoAddresses(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	nodeID := int64(0)

	mockQuery.EXPECT().SelectStrings(mockTx, "SELECT address FROM nodes WHERE id=?", nodeID).Return([]string{}, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	nodeAddress, err := cluster.NodeAddress()
	if err != nil {
		t.Errorf("expected err not to be nil")
	}
	if expected, actual := "", nodeAddress; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestClusterTxNodeAddressWithTooManyAddresses(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	nodeID := int64(0)
	input := "0.0.0.0"

	mockQuery.EXPECT().SelectStrings(mockTx, "SELECT address FROM nodes WHERE id=?", nodeID).Return([]string{input, input}, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.NodeAddress()
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodeHeartbeat(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	nodeID := int64(0)
	now := time.Now()
	address := "0.0.0.0"

	mockTx.EXPECT().Exec("UPDATE nodes SET heartbeat=? WHERE address=?", now, address).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodeHeartbeat(address, now)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterTxNodeHeartbeatWithExecFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	nodeID := int64(0)
	now := time.Now()
	address := "0.0.0.0"

	mockTx.EXPECT().Exec("UPDATE nodes SET heartbeat=? WHERE address=?", now, address).Return(mockResult, errors.New("bad"))

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodeHeartbeat(address, now)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodeHeartbeatWithMoreRowsAffected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	nodeID := int64(0)
	now := time.Now()
	address := "0.0.0.0"

	mockTx.EXPECT().Exec("UPDATE nodes SET heartbeat=? WHERE address=?", now, address).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(2), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodeHeartbeat(address, now)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodeHeartbeatWithRowsAffectedFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	nodeID := int64(0)
	now := time.Now()
	address := "0.0.0.0"

	mockTx.EXPECT().Exec("UPDATE nodes SET heartbeat=? WHERE address=?", now, address).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(2), errors.New("bad"))

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodeHeartbeat(address, now)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.NodeInfo{
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

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		"SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? ORDER BY id",
		0,
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	nodeInfo, err := cluster.Nodes()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := input, nodeInfo; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxNodesWithSelectObjectsFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.NodeInfo{
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

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		"SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? ORDER BY id",
		0,
	).Return(errors.New("bad"))

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.Nodes()
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodeAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockQuery.EXPECT().UpsertObject(mockTx, "nodes", []string{
		"name",
		"address",
		"schema",
		"api_extensions",
	}, []interface{}{
		"name",
		"0.0.0.0",
		1,
		1,
	}).Return(int64(10), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	id, err := cluster.NodeAdd("name", "0.0.0.0", 1, 1)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := int64(10), id; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxNodePending(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("UPDATE nodes SET pending=? WHERE id=?", 1, int64(1)).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodePending(int64(1), true)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterTxNodePendingWithFalse(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("UPDATE nodes SET pending=? WHERE id=?", 0, int64(1)).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodePending(int64(1), false)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterTxNodePendingWithExecFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("UPDATE nodes SET pending=? WHERE id=?", 0, int64(1)).Return(mockResult, errors.New("bad"))

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodePending(int64(1), false)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodePendingWithMoreRowsAffected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("UPDATE nodes SET pending=? WHERE id=?", 0, int64(1)).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(2), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodePending(int64(1), false)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodeUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("UPDATE nodes SET name=?, address=? WHERE id=?", "name", "0.0.0.0", nodeID).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodeUpdate(nodeID, "name", "0.0.0.0")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterTxNodeUpdateWithExecFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("UPDATE nodes SET name=?, address=? WHERE id=?", "name", "0.0.0.0", nodeID).Return(mockResult, errors.New("bad"))

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodeUpdate(nodeID, "name", "0.0.0.0")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodeUpdateWithRowAffectedFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("UPDATE nodes SET name=?, address=? WHERE id=?", "name", "0.0.0.0", nodeID).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), errors.New("bad"))

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodeUpdate(nodeID, "name", "0.0.0.0")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodeUpdateWithTooManyRowAffected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("UPDATE nodes SET name=?, address=? WHERE id=?", "name", "0.0.0.0", nodeID).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(2), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodeUpdate(nodeID, "name", "0.0.0.0")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodeRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM nodes WHERE id=?", int64(1)).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(1), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodeRemove(int64(1))
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterTxNodeRemoveWithExecFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM nodes WHERE id=?", int64(1)).Return(mockResult, errors.New("bad"))

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodeRemove(int64(1))
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodeRemoveWithMoreRowsAffected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockTx.EXPECT().Exec("DELETE FROM nodes WHERE id=?", int64(1)).Return(mockResult, nil)
	mockResult.EXPECT().RowsAffected().Return(int64(2), nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	err := cluster.NodeRemove(int64(1))
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodesByName(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.NodeInfo{
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

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		"SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? AND name=? ORDER BY id",
		0,
		"node1",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	nodeInfo, err := cluster.NodeByName("node1")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := input[0], nodeInfo; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxNodesByNameWithNoNodesReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.NodeInfo{}

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		"SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? AND name=? ORDER BY id",
		0,
		"node1",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.NodeByName("node1")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodesByNameWithMoreNodesReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.NodeInfo{
		{
			ID:            0,
			Name:          "node1",
			Address:       "0.0.0.0",
			Description:   "node 1",
			Schema:        1,
			APIExtensions: 1,
			Heartbeat:     time.Now(),
		},
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

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		"SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? AND name=? ORDER BY id",
		0,
		"node1",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.NodeByName("node1")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodePendingByAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.NodeInfo{
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

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		"SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? AND address=? ORDER BY id",
		1,
		"0.0.0.0",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	nodeInfo, err := cluster.NodePendingByAddress("0.0.0.0")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := input[0], nodeInfo; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxNodePendingByAddressWithNoNodesReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.NodeInfo{}

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		"SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? AND address=? ORDER BY id",
		1,
		"0.0.0.0",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.NodePendingByAddress("0.0.0.0")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodePendingByAddressWithMoreNodesReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	input := []db.NodeInfo{
		{
			ID:            0,
			Name:          "node1",
			Address:       "0.0.0.0",
			Description:   "node 1",
			Schema:        1,
			APIExtensions: 1,
			Heartbeat:     time.Now(),
		},
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

	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		"SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? AND address=? ORDER BY id",
		1,
		"0.0.0.0",
	).Return(nil)

	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.NodePendingByAddress("0.0.0.0")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterTxNodeCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockQuery.EXPECT().Count(mockTx, "nodes", "").Return(2, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	count, err := cluster.NodesCount()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := 2, count; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxNodeIsEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	msg, err := cluster.NodeIsEmpty(nodeID)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := "", msg; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxNodeOfflineThreshold(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockQuery.EXPECT().SelectStrings(mockTx, "SELECT value FROM config WHERE key='cluster.offline_threshold'").Return([]string{"5"}, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	threshold, err := cluster.NodeOfflineThreshold()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := 5*time.Second, threshold; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxNodeOfflineThresholdWithNoResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockQuery.EXPECT().SelectStrings(mockTx, "SELECT value FROM config WHERE key='cluster.offline_threshold'").Return([]string{}, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	threshold, err := cluster.NodeOfflineThreshold()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := db.ClusterDefaultOfflineThreshold*time.Second, threshold; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterTxNodeOfflineThresholdWithInvalidNumber(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)
	nodeID := int64(0)

	mockQuery.EXPECT().SelectStrings(mockTx, "SELECT value FROM config WHERE key='cluster.offline_threshold'").Return([]string{"aaa"}, nil)

	cluster := db.NewClusterTxWithQuery(mockTx, nodeID, mockQuery)
	_, err := cluster.NodeOfflineThreshold()
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}
