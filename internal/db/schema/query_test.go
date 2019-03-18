package schema_test

import (
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/schema"
	"github.com/spoke-d/thermionic/internal/db/schema/mocks"
)

func TestSchemaTableExists(t *testing.T) {
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

	ok, err := schema.SchemaTableExists(mockTx)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := true, ok; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}

func TestSchemaTableExistsWithNoTableFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(nil, errors.New("bad")),
	)

	ok, err := schema.SchemaTableExists(mockTx)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
	if expected, actual := false, ok; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}

func TestSchemaTableExistsWithCountNotEqualToOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, 0).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	ok, err := schema.SchemaTableExists(mockTx)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := false, ok; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}

func TestSchemaTableExistsWithScanError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).SetArg(0, 1).Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	ok, err := schema.SchemaTableExists(mockTx)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
	if expected, actual := false, ok; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}

func TestSchemaTableExistsWithNoRows(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Close().Return(nil),
	)

	ok, err := schema.SchemaTableExists(mockTx)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
	if expected, actual := false, ok; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}

func TestExecFromFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)
	mockFile := NewMockFile("SELECT * FROM schema;")

	gomock.InOrder(
		mockFileSystem.EXPECT().Exists("/path/to/a/db").Return(true),
		mockFileSystem.EXPECT().Open("/path/to/a/db").Return(mockFile, nil),
		mockTx.EXPECT().Exec("SELECT * FROM schema;").Return(nil, nil),
		mockFileSystem.EXPECT().Remove("/path/to/a/db").Return(nil),
	)

	var version int
	hook := func(v int, tx database.Tx) error {
		version = v
		return nil
	}

	err := schema.ExecFromFile(mockFileSystem, mockTx, "/path/to/a/db", hook)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
	if expected, actual := -1, version; expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

func TestExecFromFileWithOpenFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)

	gomock.InOrder(
		mockFileSystem.EXPECT().Exists("/path/to/a/db").Return(true),
		mockFileSystem.EXPECT().Open("/path/to/a/db").Return(nil, errors.New("bad")),
	)

	hook := func(int, database.Tx) error {
		t.Fail()
		return nil
	}

	err := schema.ExecFromFile(mockFileSystem, mockTx, "/path/to/a/db", hook)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestExecFromFileWithReadFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)
	mockFile := NewMockErrorFile()

	gomock.InOrder(
		mockFileSystem.EXPECT().Exists("/path/to/a/db").Return(true),
		mockFileSystem.EXPECT().Open("/path/to/a/db").Return(mockFile, nil),
	)

	hook := func(v int, tx database.Tx) error {
		t.Fail()
		return nil
	}

	err := schema.ExecFromFile(mockFileSystem, mockTx, "/path/to/a/db", hook)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestExecFromFileWithHookFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)
	mockFile := NewMockFile("SELECT * FROM schema;")

	gomock.InOrder(
		mockFileSystem.EXPECT().Exists("/path/to/a/db").Return(true),
		mockFileSystem.EXPECT().Open("/path/to/a/db").Return(mockFile, nil),
	)

	hook := func(v int, tx database.Tx) error {
		return errors.New("bad")
	}

	err := schema.ExecFromFile(mockFileSystem, mockTx, "/path/to/a/db", hook)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestExecFromFileWithExecFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTx := mocks.NewMockTx(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)
	mockFile := NewMockFile("SELECT * FROM schema;")

	gomock.InOrder(
		mockFileSystem.EXPECT().Exists("/path/to/a/db").Return(true),
		mockFileSystem.EXPECT().Open("/path/to/a/db").Return(mockFile, nil),
		mockTx.EXPECT().Exec("SELECT * FROM schema;").Return(nil, errors.New("bad")),
	)

	hook := func(v int, tx database.Tx) error {
		return nil
	}

	err := schema.ExecFromFile(mockFileSystem, mockTx, "/path/to/a/db", hook)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

type MockFile struct {
	raw   []byte
	index int64
}

func NewMockFile(sql string) *MockFile {
	return &MockFile{
		raw: []byte(sql),
	}
}

func (m *MockFile) Read(p []byte) (int, error) {
	if m.index >= int64(len(m.raw)) {
		return 0, io.EOF
	}
	n := copy(p, m.raw[m.index:])
	m.index += int64(n)
	return n, nil
}

func (m *MockFile) Write(p []byte) (int, error) {
	return 0, errors.New("bad")
}

func (m *MockFile) Name() string {
	return ""
}

func (m *MockFile) Size() int64 {
	return 0
}

func (m *MockFile) Sync() error {
	return nil
}

func (m *MockFile) Close() error {
	return nil
}

type MockErrorFile struct{}

func NewMockErrorFile() *MockErrorFile {
	return &MockErrorFile{}
}

func (m *MockErrorFile) Read(p []byte) (int, error) {
	return 0, errors.New("bad")
}

func (m *MockErrorFile) Write(p []byte) (int, error) {
	return 0, errors.New("bad")
}

func (m *MockErrorFile) Name() string {
	return ""
}

func (m *MockErrorFile) Size() int64 {
	return 0
}

func (m *MockErrorFile) Sync() error {
	return nil
}

func (m *MockErrorFile) Close() error {
	return nil
}
