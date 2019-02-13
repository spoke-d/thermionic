package query_test

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"

	"github.com/spoke-d/thermionic/internal/db/query"
	"github.com/spoke-d/thermionic/internal/db/query/mocks"
	"github.com/golang/mock/gomock"
)

func TestSelectConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT key, value FROM config").Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(StringScanMatcher("foo"), StringScanMatcher("bar")).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	records, err := query.SelectConfig(mockTx, "config", "")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := map[string]string{"foo": "bar"}, records; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestSelectConfigWithWhereClause(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT key, value FROM config WHERE value=?", "bar").Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(StringScanMatcher("foo"), StringScanMatcher("bar")).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	records, err := query.SelectConfig(mockTx, "config", "value=?", "bar")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := map[string]string{"foo": "bar"}, records; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestSelectConfigWithQueryFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT key, value FROM config").Return(mockRows, errors.New("bad")),
	)

	_, err := query.SelectConfig(mockTx, "config", "")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestSelectConfigWithScanFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT key, value FROM config").Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(StringScanMatcher("foo"), StringScanMatcher("bar")).Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.SelectConfig(mockTx, "config", "")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestUpdateObject(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", []interface{}{"foo", "bar"}).Return(mockResult, nil),
		mockTx.EXPECT().Exec("DELETE FROM config WHERE key IN (?)", "baz").Return(mockResult, nil),
	)

	err := query.UpdateConfig(mockTx, "config", map[string]string{"foo": "bar", "baz": ""})
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestUpdateObjectWithUpsertFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", []interface{}{"foo", "bar"}).Return(mockResult, errors.New("bad")),
	)

	err := query.UpdateConfig(mockTx, "config", map[string]string{"foo": "bar", "baz": ""})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestUpdateObjectWithDeleteFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", []interface{}{"foo", "bar"}).Return(mockResult, nil),
		mockTx.EXPECT().Exec("DELETE FROM config WHERE key IN (?)", "baz").Return(mockResult, errors.New("bad")),
	)

	err := query.UpdateConfig(mockTx, "config", map[string]string{"foo": "bar", "baz": ""})
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}
