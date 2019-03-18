package query_test

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"

	"github.com/golang/mock/gomock"
	"github.com/spoke-d/thermionic/internal/db/query"
	"github.com/spoke-d/thermionic/internal/db/query/mocks"
)

func TestSelectObjects(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(IntScanMatcher(1), IntScanMatcher(2)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	record := make([]int, 2)
	err := query.SelectObjects(mockTx, func(i int) []interface{} {
		return []interface{}{
			&record[0],
			&record[1],
		}
	}, "SELECT * FROM schema")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := []int{1, 2}, record; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestSelectObjectsWithQueryFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, errors.New("bad")),
	)

	err := query.SelectObjects(mockTx, func(i int) []interface{} {
		return []interface{}{}
	}, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestSelectObjectsWithScanFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(IntScanMatcher(1), IntScanMatcher(2)).Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	record := make([]int, 2)
	err := query.SelectObjects(mockTx, func(i int) []interface{} {
		return []interface{}{
			&record[0],
			&record[1],
		}
	}, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestUpsertObject(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec("INSERT OR REPLACE INTO schema (id, name) VALUES (?, ?)", []interface{}{1, "fred"}).Return(mockResult, nil),
		mockResult.EXPECT().LastInsertId().Return(int64(5), nil),
	)

	id, err := query.UpsertObject(mockTx, "schema", []string{"id", "name"}, []interface{}{1, "fred"})
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := int64(5), id; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestUpsertObjectWithExecFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec("INSERT OR REPLACE INTO schema (id, name) VALUES (?, ?)", []interface{}{1, "fred"}).Return(mockResult, errors.New("bad")),
	)

	_, err := query.UpsertObject(mockTx, "schema", []string{"id", "name"}, []interface{}{1, "fred"})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestUpsertObjectWithLastInsertedIdFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec("INSERT OR REPLACE INTO schema (id, name) VALUES (?, ?)", []interface{}{1, "fred"}).Return(mockResult, nil),
		mockResult.EXPECT().LastInsertId().Return(int64(5), errors.New("bad")),
	)

	_, err := query.UpsertObject(mockTx, "schema", []string{"id", "name"}, []interface{}{1, "fred"})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestUpsertObjectWithNoColumns(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	_, err := query.UpsertObject(mockTx, "schema", []string{}, []interface{}{1, "fred"})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestUpsertObjectWithColumnsValuesMissmatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	_, err := query.UpsertObject(mockTx, "schema", []string{"id"}, []interface{}{1, "fred"})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestDeleteObject(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec("DELETE FROM schema WHERE id=?", int64(1)).Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil),
	)

	ok, err := query.DeleteObject(mockTx, "schema", int64(1))
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := true, ok; expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestDeleteObjectWithExecFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec("DELETE FROM schema WHERE id=?", int64(1)).Return(mockResult, errors.New("bad")),
	)

	_, err := query.DeleteObject(mockTx, "schema", int64(1))
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestDeleteObjectWithRowsAffectedFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec("DELETE FROM schema WHERE id=?", int64(1)).Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(1), errors.New("bad")),
	)

	_, err := query.DeleteObject(mockTx, "schema", int64(1))
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestDeleteObjectWithTooManyRowsAffected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec("DELETE FROM schema WHERE id=?", int64(1)).Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(2), nil),
	)

	_, err := query.DeleteObject(mockTx, "schema", int64(1))
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}
