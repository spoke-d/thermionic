package node_test

import (
	"testing"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/node"
	"github.com/spoke-d/thermionic/internal/db/node/mocks"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
)

type nodeDeps struct {
	node           *node.Node
	databaseIO     *mocks.MockDatabaseIO
	schemaProvider *mocks.MockSchemaProvider
	fileSystem     *mocks.MockFileSystem
}

func createNodeDeps(t *testing.T, ctrl *gomock.Controller) nodeDeps {
	t.Helper()

	deps := nodeDeps{
		databaseIO:     mocks.NewMockDatabaseIO(ctrl),
		schemaProvider: mocks.NewMockSchemaProvider(ctrl),
		fileSystem:     mocks.NewMockFileSystem(ctrl),
	}

	deps.node = node.NewNodeWithMocks(
		deps.databaseIO,
		deps.schemaProvider,
		deps.fileSystem,
	)
	return deps
}

// New
func TestNew(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	fileSystem := mocks.NewMockFileSystem(ctrl)

	node := node.New(fileSystem)
	if node == nil {
		t.Errorf("expected node not be nil")
	}
}

func TestOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)

	deps := createNodeDeps(t, ctrl)
	gomock.InOrder(
		deps.databaseIO.EXPECT().Drivers().Return([]string{}),
		deps.databaseIO.EXPECT().Register(database.DriverName(), gomock.Any()),
		deps.databaseIO.EXPECT().Open(database.DriverName(), "/path/to/a/dir/local.db?_busy_timeout=0&_txlock=exclusive").Return(mockDB, nil),
	)

	err := deps.node.Open("/path/to/a/dir")
	if err != nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := mockDB, deps.node.DB(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestOpenWithEmptyName(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)

	deps := createNodeDeps(t, ctrl)
	gomock.InOrder(
		deps.databaseIO.EXPECT().Drivers().Return([]string{}),
		deps.databaseIO.EXPECT().Register(database.DriverName(), gomock.Any()),
		deps.databaseIO.EXPECT().Open(database.DriverName(), "local.db?_busy_timeout=0&_txlock=exclusive").Return(mockDB, nil),
	)

	err := deps.node.Open("")
	if err != nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := mockDB, deps.node.DB(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestOpenWithErrorFromOpening(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	deps := createNodeDeps(t, ctrl)
	gomock.InOrder(
		deps.databaseIO.EXPECT().Drivers().Return([]string{}),
		deps.databaseIO.EXPECT().Register(database.DriverName(), gomock.Any()),
		deps.databaseIO.EXPECT().Open(database.DriverName(), "local.db?_busy_timeout=0&_txlock=exclusive").Return(nil, errors.New("bad")),
	)

	err := deps.node.Open("")
	if err == nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

// EnsureSchema

func TestEnsureSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockSchema := mocks.NewMockSchema(ctrl)

	deps := createNodeDeps(t, ctrl)
	gomock.InOrder(
		deps.databaseIO.EXPECT().Drivers().Return([]string{}),
		deps.databaseIO.EXPECT().Register(database.DriverName(), gomock.Any()),
		deps.databaseIO.EXPECT().Open(database.DriverName(), "/path/to/a/dir/local.db?_busy_timeout=0&_txlock=exclusive").Return(mockDB, nil),
		deps.schemaProvider.EXPECT().Schema().Return(mockSchema),
		mockSchema.EXPECT().File("/path/to/a/dir/patch.local.sql"),
		mockSchema.EXPECT().Hook(gomock.Any()),
		mockSchema.EXPECT().Ensure(mockDB).Return(0, nil),
	)

	err := deps.node.Open("/path/to/a/dir")
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	version, err := deps.node.EnsureSchema(func(version int, tx database.Tx) error {
		return nil
	})
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	if expected, actual := 0, version; expected != actual {
		t.Errorf("expected: %d, actual: %d", expected, actual)
	}
}

// hook

func TestHook(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &node.Context{}
	mockTx := mocks.NewMockTx(ctrl)

	deps := createNodeDeps(t, ctrl)
	gomock.InOrder(
		deps.fileSystem.EXPECT().CopyFile("/path/to/a/dir/local.db", "/path/to/a/dir/local.db.bak").Return(nil),
	)

	var called bool
	err := node.Hook(ctx, deps.fileSystem, func(version int, tx database.Tx) error {
		called = true
		return nil
	}, "/path/to/a/dir", 1, mockTx)
	if err != nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := true, called; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}

func TestHookWithCopyFileFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &node.Context{}
	mockTx := mocks.NewMockTx(ctrl)

	deps := createNodeDeps(t, ctrl)
	gomock.InOrder(
		deps.fileSystem.EXPECT().CopyFile("/path/to/a/dir/local.db", "/path/to/a/dir/local.db.bak").Return(errors.New("bad")),
	)

	err := node.Hook(ctx, deps.fileSystem, func(version int, tx database.Tx) error {
		t.Fail()
		return nil
	}, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
}

func TestHookWithHookFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &node.Context{}
	mockTx := mocks.NewMockTx(ctrl)

	deps := createNodeDeps(t, ctrl)
	gomock.InOrder(
		deps.fileSystem.EXPECT().CopyFile("/path/to/a/dir/local.db", "/path/to/a/dir/local.db.bak").Return(nil),
	)

	err := node.Hook(ctx, deps.fileSystem, func(version int, tx database.Tx) error {
		return errors.New("bad")
	}, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
}
