package query_test

import (
	"database/sql"
	"testing"

	"github.com/pkg/errors"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/query"
	"github.com/spoke-d/thermionic/internal/db/query/mocks"
	"github.com/golang/mock/gomock"
)

func TestTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockTx.EXPECT().Commit().Return(nil),
	)

	var called bool
	err := query.Transaction(mockDB, func(tx database.Tx) error {
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

func TestTransactionWithBeginFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(nil, errors.New("bad")),
	)

	err := query.Transaction(mockDB, func(tx database.Tx) error {
		t.Fail()
		return nil
	})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestTransactionWithFuncFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockTx.EXPECT().Rollback().Return(nil),
	)

	err := query.Transaction(mockDB, func(tx database.Tx) error {
		return errors.New("bad")
	})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestTransactionWithRollbackFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockTx.EXPECT().Rollback().Return(errors.New("bad")),
	)

	err := query.Transaction(mockDB, func(tx database.Tx) error {
		return errors.New("bad")
	})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestTransactionWithCommitTxFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockTx.EXPECT().Commit().Return(sql.ErrTxDone),
	)

	err := query.Transaction(mockDB, func(tx database.Tx) error {
		return nil
	})
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestTransactionWithCommitFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockTx.EXPECT().Commit().Return(errors.New("bad")),
	)

	err := query.Transaction(mockDB, func(tx database.Tx) error {
		return nil
	})
	if err == nil {
		t.Errorf("expected err to be nil")
	}
}
