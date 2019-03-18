package db_test

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/mocks"
)

func TestNodeOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQueryNode := mocks.NewMockQueryNode(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	gomock.InOrder(
		mockQueryNode.EXPECT().Open("/path/to/a/dir").Return(nil),
		mockQueryNode.EXPECT().EnsureSchema(gomock.Any()).Return(0, nil),
	)

	var called bool
	node := db.NewNodeWithQuery(mockQueryNode, mockTransaction, db.NewNodeTx)
	err := node.Open("/path/to/a/dir", func(n *db.Node) error {
		called = true
		return nil
	})
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := true, called; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
	if node == nil {
		t.Errorf("expected node to no be nil")
	}
}

func TestNodeOpenWithOpenFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQueryNode := mocks.NewMockQueryNode(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	gomock.InOrder(
		mockQueryNode.EXPECT().Open("/path/to/a/dir").Return(errors.New("bad")),
	)

	node := db.NewNodeWithQuery(mockQueryNode, mockTransaction, db.NewNodeTx)
	err := node.Open("/path/to/a/dir", func(n *db.Node) error {
		t.Fail()
		return nil
	})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestNodeNodeWithEnsureSchemaFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQueryNode := mocks.NewMockQueryNode(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	gomock.InOrder(
		mockQueryNode.EXPECT().Open("/path/to/a/dir").Return(nil),
		mockQueryNode.EXPECT().EnsureSchema(gomock.Any()).Return(0, errors.New("bad")),
	)

	node := db.NewNodeWithQuery(mockQueryNode, mockTransaction, db.NewNodeTx)
	err := node.Open("/path/to/a/dir", func(n *db.Node) error {
		t.Fail()
		return nil
	})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestNodeNodeWithHookFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQueryNode := mocks.NewMockQueryNode(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	gomock.InOrder(
		mockQueryNode.EXPECT().Open("/path/to/a/dir").Return(nil),
		mockQueryNode.EXPECT().EnsureSchema(gomock.Any()).Return(0, nil),
	)

	node := db.NewNodeWithQuery(mockQueryNode, mockTransaction, db.NewNodeTx)
	err := node.Open("/path/to/a/dir", func(n *db.Node) error {
		return errors.New("bad")
	})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestNodeTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockQueryNode := mocks.NewMockQueryNode(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	gomock.InOrder(
		mockQueryNode.EXPECT().DB().Return(mockDB),
		mockTransaction.EXPECT().Transaction(mockDB, TransactionMatcher(mockTx)).Return(nil),
	)

	var called bool
	node := db.NewNodeWithQuery(mockQueryNode, mockTransaction, db.NewNodeTx)
	err := node.Transaction(func(n *db.NodeTx) error {
		called = true
		return nil
	})
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := true, called; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}
