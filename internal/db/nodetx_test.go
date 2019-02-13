package db_test

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/mocks"
	"github.com/golang/mock/gomock"
)

func TestNodeTxWithConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := map[string]string{
		"foo": "bar",
		"baz": "do",
	}
	gomock.InOrder(
		mockQuery.EXPECT().SelectConfig(mockTx, "config", "").Return(want, nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	values, err := node.Config()
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := want, values; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestNodeTxWithUpdateConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := map[string]string{
		"foo": "bar",
		"baz": "do",
	}
	gomock.InOrder(
		mockQuery.EXPECT().UpdateConfig(mockTx, "config", want).Return(nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	err := node.UpdateConfig(want)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestNodeTxWithRaftNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	gomock.InOrder(
		mockQuery.EXPECT().SelectObjects(mockTx, NodeDestSelectObjectsMatcher(map[int64]string{
			1: "https://10.0.0.123",
			2: "https://10.0.0.223",
		}), "SELECT id, address FROM raft_nodes ORDER BY id").Return(nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	nodes, err := node.RaftNodes()
	if err != nil {
		t.Errorf("expected err to be nil")
	}

	want := []db.RaftNode{
		{
			ID:      int64(1),
			Address: "https://10.0.0.123",
		},
		{
			ID:      int64(2),
			Address: "https://10.0.0.223",
		},
	}
	if expected, actual := want, nodes; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestNodeTxWithRaftNodesWithSelectObjectsFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	gomock.InOrder(
		mockQuery.EXPECT().SelectObjects(mockTx, NodeDestSelectObjectsMatcher(map[int64]string{}), "SELECT id, address FROM raft_nodes ORDER BY id").Return(errors.New("bad")),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	_, err := node.RaftNodes()
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestNodeTxWithRaftNodeAddresses(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := []string{
		"https://10.0.0.123",
		"https://10.0.0.223",
	}
	gomock.InOrder(
		mockQuery.EXPECT().SelectStrings(mockTx, "SELECT address FROM raft_nodes ORDER BY id").Return(want, nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	nodes, err := node.RaftNodeAddresses()
	if err != nil {
		t.Errorf("expected err to be nil")
	}

	if expected, actual := want, nodes; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestNodeTxWithRaftNodeAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := []string{
		"https://10.0.0.123",
	}
	gomock.InOrder(
		mockQuery.EXPECT().SelectStrings(mockTx, "SELECT address FROM raft_nodes WHERE id=?", int64(1)).Return(want, nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	address, err := node.RaftNodeAddress(1)
	if err != nil {
		t.Errorf("expected err to be nil")
	}

	if expected, actual := want[0], address; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestNodeTxWithRaftNodeAddressWithZeroResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := []string{}
	gomock.InOrder(
		mockQuery.EXPECT().SelectStrings(mockTx, "SELECT address FROM raft_nodes WHERE id=?", int64(1)).Return(want, nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	_, err := node.RaftNodeAddress(1)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
	if err != db.ErrNoSuchObject {
		t.Errorf("expected err to be NoSuchObject")
	}
}

func TestNodeTxWithRaftNodeAddressWithMultipleRecords(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := []string{
		"https://10.0.0.123",
		"https://10.0.0.223",
	}
	gomock.InOrder(
		mockQuery.EXPECT().SelectStrings(mockTx, "SELECT address FROM raft_nodes WHERE id=?", int64(1)).Return(want, nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	_, err := node.RaftNodeAddress(1)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestNodeTxWithRaftNodeFirst(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := "https://10.0.0.123"
	gomock.InOrder(
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id",
			"address",
		}, []interface{}{
			int64(1),
			want,
		}).Return(int64(1), nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	err := node.RaftNodeFirst(want)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestNodeTxWithRaftNodeFirstWithUpsertObjectFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := "https://10.0.0.123"
	gomock.InOrder(
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id",
			"address",
		}, []interface{}{
			int64(1),
			want,
		}).Return(int64(1), errors.New("bad")),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	err := node.RaftNodeFirst(want)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestNodeTxWithRaftNodeFirstWithIDNotEqualToOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := "https://10.0.0.123"
	gomock.InOrder(
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id",
			"address",
		}, []interface{}{
			int64(1),
			want,
		}).Return(int64(2), nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	err := node.RaftNodeFirst(want)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestNodeTxWithRaftNodeAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := "https://10.0.0.123"
	gomock.InOrder(
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"address",
		}, []interface{}{
			want,
		}).Return(int64(2), nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	id, err := node.RaftNodeAdd(want)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := int64(2), id; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestNodeTxWithRaftNodeDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := int64(2)
	gomock.InOrder(
		mockQuery.EXPECT().DeleteObject(mockTx, "raft_nodes", want).Return(true, nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	err := node.RaftNodeDelete(want)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestNodeTxWithRaftNodeDeleteWithNoSuchObject(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := int64(2)
	gomock.InOrder(
		mockQuery.EXPECT().DeleteObject(mockTx, "raft_nodes", want).Return(false, nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	err := node.RaftNodeDelete(want)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
	if err != db.ErrNoSuchObject {
		t.Errorf("expected err to be NoSuchObject")
	}
}

func TestNodeTxWithRaftNodeDeleteWithFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := int64(2)
	gomock.InOrder(
		mockQuery.EXPECT().DeleteObject(mockTx, "raft_nodes", want).Return(false, errors.New("bad")),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	err := node.RaftNodeDelete(want)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestNodeTxWithRaftNodesReplace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := []db.RaftNode{
		{
			ID:      int64(1),
			Address: "https://10.0.0.123",
		},
	}
	gomock.InOrder(
		mockTx.EXPECT().Exec("DELETE FROM raft_nodes").Return(nil, nil),
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id",
			"address",
		}, []interface{}{
			int64(1),
			"https://10.0.0.123",
		}).Return(int64(1), nil),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	err := node.RaftNodesReplace(want)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestNodeTxWithRaftNodesReplaceWithExecFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := []db.RaftNode{
		{
			ID:      int64(1),
			Address: "https://10.0.0.123",
		},
	}
	gomock.InOrder(
		mockTx.EXPECT().Exec("DELETE FROM raft_nodes").Return(nil, errors.New("bad")),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	err := node.RaftNodesReplace(want)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestNodeTxWithRaftNodesReplaceWithUpsertFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQuery := mocks.NewMockQuery(ctrl)

	want := []db.RaftNode{
		{
			ID:      int64(1),
			Address: "https://10.0.0.123",
		},
	}
	gomock.InOrder(
		mockTx.EXPECT().Exec("DELETE FROM raft_nodes").Return(nil, nil),
		mockQuery.EXPECT().UpsertObject(mockTx, "raft_nodes", []string{
			"id",
			"address",
		}, []interface{}{
			int64(1),
			"https://10.0.0.123",
		}).Return(int64(1), errors.New("bad")),
	)

	node := db.NewNodeTxWithQuery(mockTx, mockQuery)
	err := node.RaftNodesReplace(want)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}
