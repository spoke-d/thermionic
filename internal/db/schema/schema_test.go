package schema_test

import (
	"reflect"
	"testing"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/schema"
	"github.com/spoke-d/thermionic/internal/db/schema/mocks"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
)

func TestNewSchema(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	s := schema.New(mockFileSystem, []schema.Update{})
	if s == nil {
		t.Errorf("expected schema to not be nil")
	}
	if expected, actual := 0, s.Len(); expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

func TestNewEmptySchema(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	s := schema.Empty(mockFileSystem)
	if s == nil {
		t.Errorf("expected schema to not be nil")
	}
	if expected, actual := 0, s.Len(); expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

func TestNewSchemaWithAdd(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	s := schema.New(mockFileSystem, []schema.Update{})
	s.Add(func(database.Tx) error {
		return nil
	})
	if expected, actual := 1, s.Len(); expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

func TestNewSchemaWithTrim(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	var record []int
	define := func(v int) func(database.Tx) error {
		return func(database.Tx) error {
			record = append(record, v)
			return nil
		}
	}

	s := schema.New(mockFileSystem, []schema.Update{})
	s.Add(define(1))
	s.Add(define(2))
	s.Add(define(3))
	s.Add(define(4))
	if expected, actual := 4, s.Len(); expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}

	res := s.Trim(2)
	if expected, actual := 2, s.Len(); expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}

	for _, v := range res {
		if err := v(mockTx); err != nil {
			t.Errorf("expected err to be nil")
		}
	}

	if expected, actual := []int{3, 4}, record; !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestSchemaEnsure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockFileSystem.EXPECT().Exists("/path/to/a/file").Return(false),
		expectSchemaTableExists(mockTx, mockRows, 1),
		expectCurrentVersion(ctrl, mockTx, mockRows, 1),
		mockTx.EXPECT().Commit().Return(nil),
	)

	schema := schema.New(mockFileSystem, []schema.Update{
		func(database.Tx) error {
			return nil
		},
	})
	schema.File("/path/to/a/file")
	current, err := schema.Ensure(mockDB)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := 1, current; expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

func TestSchemaEnsureCreatesNewSchemaTable(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockFileSystem.EXPECT().Exists("/path/to/a/file").Return(false),
		expectSchemaTableExists(mockTx, mockRows, 0),
		mockTx.EXPECT().Exec(schema.StmtCreateTable).Return(nil, nil),
		expectCurrentVersion(ctrl, mockTx, mockRows, 1),
		mockTx.EXPECT().Commit().Return(nil),
	)

	schema := schema.New(mockFileSystem, []schema.Update{
		func(database.Tx) error {
			return nil
		},
	})
	schema.File("/path/to/a/file")
	current, err := schema.Ensure(mockDB)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := 1, current; expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

func TestSchemaEnsureCallsCheck(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockFileSystem.EXPECT().Exists("/path/to/a/file").Return(false),
		expectSchemaTableExists(mockTx, mockRows, 1),
		expectCurrentVersion(ctrl, mockTx, mockRows, 1),
		mockTx.EXPECT().Commit().Return(nil),
	)

	var called bool
	schema := schema.New(mockFileSystem, []schema.Update{
		func(database.Tx) error {
			return nil
		},
	})
	schema.File("/path/to/a/file")
	schema.Check(func(current int, tx database.Tx) error {
		called = true
		return nil
	})
	current, err := schema.Ensure(mockDB)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := 1, current; expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
	if expected, actual := true, called; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}

func TestSchemaEnsureCallsCheckWithFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockFileSystem.EXPECT().Exists("/path/to/a/file").Return(false),
		expectSchemaTableExists(mockTx, mockRows, 1),
		expectCurrentVersion(ctrl, mockTx, mockRows, 1),
		mockTx.EXPECT().Rollback().Return(nil),
	)

	schema := schema.New(mockFileSystem, []schema.Update{
		func(database.Tx) error {
			return nil
		},
	})
	schema.File("/path/to/a/file")
	schema.Check(func(current int, tx database.Tx) error {
		return errors.New("bad")
	})
	_, err := schema.Ensure(mockDB)
	if err == nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestSchemaEnsureCallsCheckWithAbortedFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockFileSystem.EXPECT().Exists("/path/to/a/file").Return(false),
		expectSchemaTableExists(mockTx, mockRows, 1),
		expectCurrentVersion(ctrl, mockTx, mockRows, 1),
		mockTx.EXPECT().Commit().Return(nil),
	)

	errGracefulAbort := schema.ErrGracefulAbort
	schema := schema.New(mockFileSystem, []schema.Update{
		func(database.Tx) error {
			return nil
		},
	})
	schema.File("/path/to/a/file")
	schema.Check(func(current int, tx database.Tx) error {
		return errGracefulAbort
	})
	_, err := schema.Ensure(mockDB)
	if err == nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	if expected, actual := errGracefulAbort, errors.Cause(err); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestSchemaEnsureCallsFresh(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockFileSystem.EXPECT().Exists("/path/to/a/file").Return(false),
		expectSchemaTableExists(mockTx, mockRows, 0),
		mockTx.EXPECT().Exec(schema.StmtCreateTable).Return(nil, nil),
		expectCurrentVersion(ctrl, mockTx, mockRows, 0),
		mockTx.EXPECT().Exec("SELECT * FROM nodes").Return(nil, nil),
		mockTx.EXPECT().Commit().Return(nil),
	)

	schema := schema.New(mockFileSystem, []schema.Update{
		func(database.Tx) error {
			return nil
		},
	})
	schema.File("/path/to/a/file")
	schema.Fresh("SELECT * FROM nodes")
	current, err := schema.Ensure(mockDB)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := 0, current; expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

func TestSchemaEnsureCallsHook(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockFileSystem.EXPECT().Exists("/path/to/a/file").Return(false),
		expectSchemaTableExists(mockTx, mockRows, 0),
		mockTx.EXPECT().Exec(schema.StmtCreateTable).Return(nil, nil),
		expectCurrentVersion(ctrl, mockTx, mockRows, 1),
		mockTx.EXPECT().Exec(schema.StmtInsertSchemaVersion, 2).Return(nil, nil),
		mockTx.EXPECT().Commit().Return(nil),
	)

	var called bool
	schema := schema.New(mockFileSystem, []schema.Update{
		func(database.Tx) error {
			return nil
		},
	})
	schema.File("/path/to/a/file")
	schema.Add(func(database.Tx) error {
		return nil
	})
	schema.Hook(func(current int, tx database.Tx) error {
		called = true
		return nil
	})
	current, err := schema.Ensure(mockDB)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := 1, current; expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
	if expected, actual := true, called; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}

func TestSchemaEnsureCallsHookFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	gomock.InOrder(
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		mockFileSystem.EXPECT().Exists("/path/to/a/file").Return(false),
		expectSchemaTableExists(mockTx, mockRows, 0),
		mockTx.EXPECT().Exec(schema.StmtCreateTable).Return(nil, nil),
		expectCurrentVersion(ctrl, mockTx, mockRows, 0),
		mockTx.EXPECT().Rollback().Return(nil),
	)

	schema := schema.New(mockFileSystem, []schema.Update{
		func(database.Tx) error {
			return nil
		},
	})
	schema.File("/path/to/a/file")
	schema.Hook(func(current int, tx database.Tx) error {
		return errors.New("bad")
	})
	_, err := schema.Ensure(mockDB)
	if err == nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestQueryCurrentVersion(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockColumnType := mocks.NewMockColumnType(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSelectSchemaVersions).Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return([]database.ColumnType{
			mockColumnType,
		}, nil),
		mockColumnType.EXPECT().DatabaseTypeName().Return("INTEGER"),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, 1).Return(nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, 2).Return(nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, 3).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	current, err := schema.QueryCurrentVersion(mockTx)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := 3, current; expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

func TestQueryCurrentVersionWithVersionHole(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockColumnType := mocks.NewMockColumnType(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSelectSchemaVersions).Return(mockRows, nil),
		mockRows.EXPECT().ColumnTypes().Return([]database.ColumnType{
			mockColumnType,
		}, nil),
		mockColumnType.EXPECT().DatabaseTypeName().Return("INTEGER"),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, 1).Return(nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, 3).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	_, err := schema.QueryCurrentVersion(mockTx)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestQueryCurrentVersionWithSelectVersionError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSelectSchemaVersions).Return(mockRows, errors.New("bad")),
	)

	_, err := schema.QueryCurrentVersion(mockTx)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestEnsureUpdatesAreApplied(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec(schema.StmtInsertSchemaVersion, 1).Return(nil, nil),
	)

	var version int
	hook := func(v int, tx database.Tx) error {
		version = v
		return nil
	}

	var called bool
	err := schema.EnsureUpdatesAreApplied(mockTx, 0, []schema.Update{
		func(database.Tx) error {
			called = true
			return nil
		},
	}, hook)
	if err != nil {
		t.Errorf("expected err not to be nil")
	}
	if expected, actual := 0, version; expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
	if expected, actual := true, called; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}

func TestEnsureUpdatesAreAppliedWithCurrent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	hook := func(v int, tx database.Tx) error {
		t.Fail()
		return nil
	}

	err := schema.EnsureUpdatesAreApplied(mockTx, 2, []schema.Update{
		func(database.Tx) error {
			return nil
		},
	}, hook)
	if err == nil {
		t.Errorf("expected err to be nil")
	}
}

func TestEnsureUpdatesAreAppliedWithNoUpdates(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	hook := func(v int, tx database.Tx) error {
		t.Fail()
		return nil
	}

	err := schema.EnsureUpdatesAreApplied(mockTx, 0, []schema.Update{}, hook)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestEnsureUpdatesAreAppliedWithUpdateFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	hook := func(v int, tx database.Tx) error {
		return nil
	}

	err := schema.EnsureUpdatesAreApplied(mockTx, 0, []schema.Update{
		func(database.Tx) error {
			return errors.New("bad")
		},
	}, hook)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestEnsureUpdatesAreAppliedWithInsertFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Exec(schema.StmtInsertSchemaVersion, 1).Return(nil, errors.New("bad")),
	)

	hook := func(v int, tx database.Tx) error {
		return nil
	}

	err := schema.EnsureUpdatesAreApplied(mockTx, 0, []schema.Update{
		func(database.Tx) error {
			return nil
		},
	}, hook)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestEnsureSchemaTableExists(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, 1).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := schema.EnsureSchemaTableExists(mockTx)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestEnsureSchemaTableExistsWithSelectFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(nil, errors.New("bad")),
	)

	err := schema.EnsureSchemaTableExists(mockTx)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestEnsureSchemaTableExistsWithCreate(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, 0).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(schema.StmtCreateTable).Return(nil, nil),
	)

	err := schema.EnsureSchemaTableExists(mockTx)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestEnsureSchemaTableExistsWithCreateFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, 0).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(schema.StmtCreateTable).Return(nil, errors.New("bad")),
	)

	err := schema.EnsureSchemaTableExists(mockTx)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}
