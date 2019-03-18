package query_test

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/db/query"
	"github.com/spoke-d/thermionic/internal/db/query/mocks"
)

func TestCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("schema", "a=b")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(IntScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	count, err := query.Count(mockTx, "schema", "a=b")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := 1, count; expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

func TestCountWithQueryFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("schema", "a=b")).Return(mockRows, errors.New("bad")),
	)

	_, err := query.Count(mockTx, "schema", "a=b")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestCountWithNoNext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("schema", "a=b")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.Count(mockTx, "schema", "a=b")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestCountWithScanFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("schema", "a=b")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(IntScanMatcher(1)).Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.Count(mockTx, "schema", "a=b")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestCountWithMoreRows(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("schema", "a=b")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(IntScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.Count(mockTx, "schema", "a=b")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestCountWithErrFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("schema", "a=b")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(IntScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.Count(mockTx, "schema", "a=b")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}
