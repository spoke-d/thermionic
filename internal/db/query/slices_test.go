package query_test

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/query"
	"github.com/spoke-d/thermionic/internal/db/query/mocks"
	"github.com/golang/mock/gomock"
)

func TestSelectStrings(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockColumnType := mocks.NewMockColumnType(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return([]database.ColumnType{
			mockColumnType,
		}, nil),
		mockColumnType.EXPECT().DatabaseTypeName().Return("TEXT"),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(StringScanMatcher("a")).Return(nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(StringScanMatcher("b")).Return(nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(StringScanMatcher("c")).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	values, err := query.SelectStrings(mockTx, "SELECT * FROM schema")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := []string{"a", "b", "c"}, values; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestSelectStringsWithScanFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockColumnType := mocks.NewMockColumnType(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return([]database.ColumnType{
			mockColumnType,
		}, nil),
		mockColumnType.EXPECT().DatabaseTypeName().Return("TEXT"),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(StringScanMatcher("a")).Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.SelectStrings(mockTx, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestSelectStringsWithQueryFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(nil, errors.New("bad")),
	)

	_, err := query.SelectStrings(mockTx, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestSelectStringsWithColumnTypesFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return(nil, errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.SelectStrings(mockTx, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestSelectStringsWithMultipleColumnTypes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockColumnType := mocks.NewMockColumnType(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return([]database.ColumnType{
			mockColumnType,
			mockColumnType,
		}, nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.SelectStrings(mockTx, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestSelectStringsWithInvalidColumnType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockColumnType := mocks.NewMockColumnType(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return([]database.ColumnType{
			mockColumnType,
		}, nil),
		mockColumnType.EXPECT().DatabaseTypeName().Return("bad"),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.SelectStrings(mockTx, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestSelectIntegers(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockColumnType := mocks.NewMockColumnType(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return([]database.ColumnType{
			mockColumnType,
		}, nil),
		mockColumnType.EXPECT().DatabaseTypeName().Return("INTEGER"),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(IntScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(IntScanMatcher(2)).Return(nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(IntScanMatcher(3)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	values, err := query.SelectIntegers(mockTx, "SELECT * FROM schema")
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := []int{1, 2, 3}, values; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestSelectIntegersWithScanFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockColumnType := mocks.NewMockColumnType(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return([]database.ColumnType{
			mockColumnType,
		}, nil),
		mockColumnType.EXPECT().DatabaseTypeName().Return("INTEGER"),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(IntScanMatcher(1)).Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.SelectIntegers(mockTx, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestSelectIntegersWithQueryFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(nil, errors.New("bad")),
	)

	_, err := query.SelectIntegers(mockTx, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestSelectIntegersWithColumnTypesFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return(nil, errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.SelectIntegers(mockTx, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestSelectIntegersWithMultipleColumnTypes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockColumnType := mocks.NewMockColumnType(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return([]database.ColumnType{
			mockColumnType,
			mockColumnType,
		}, nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.SelectIntegers(mockTx, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestSelectIntegersWithInvalidColumnType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockColumnType := mocks.NewMockColumnType(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query("SELECT * FROM schema").Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return([]database.ColumnType{
			mockColumnType,
		}, nil),
		mockColumnType.EXPECT().DatabaseTypeName().Return("bad"),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := query.SelectIntegers(mockTx, "SELECT * FROM schema")
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestInsertStrings(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec("INSERT INTO test(name) VALUES (?), (?)", []interface{}{"xx", "yy"}).Return(nil, nil),
	)

	err := query.InsertStrings(mockTx, "INSERT INTO test(name) VALUES %s", []string{"xx", "yy"})
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}
