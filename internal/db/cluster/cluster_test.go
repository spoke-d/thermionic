package cluster_test

import (
	"testing"
	"time"

	"github.com/spoke-d/thermionic/internal/db/cluster"
	"github.com/spoke-d/thermionic/internal/db/cluster/mocks"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/query"
	"github.com/spoke-d/thermionic/internal/db/schema"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
)

type clusterDeps struct {
	cluster        *cluster.Cluster
	databaseIO     *mocks.MockDatabaseIO
	nameProvider   *mocks.MockNameProvider
	schemaProvider *mocks.MockSchemaProvider
	apiExtensions  *mocks.MockAPIExtensions
	fileSystem     *mocks.MockFileSystem
	sleeper        *mocks.MockSleeper
}

func createClusterDeps(t *testing.T, ctrl *gomock.Controller) clusterDeps {
	t.Helper()

	deps := clusterDeps{
		databaseIO:     mocks.NewMockDatabaseIO(ctrl),
		nameProvider:   mocks.NewMockNameProvider(ctrl),
		schemaProvider: mocks.NewMockSchemaProvider(ctrl),
		apiExtensions:  mocks.NewMockAPIExtensions(ctrl),
		fileSystem:     mocks.NewMockFileSystem(ctrl),
		sleeper:        mocks.NewMockSleeper(ctrl),
	}

	deps.cluster = cluster.New(
		deps.apiExtensions,
		deps.schemaProvider,
		cluster.WithDatabaseIO(deps.databaseIO),
		cluster.WithNameProvider(deps.nameProvider),
		cluster.WithFileSystem(deps.fileSystem),
		cluster.WithSleeper(deps.sleeper),
	)
	return deps
}

// New
func TestNew(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	mockFileSystem := mocks.NewMockFileSystem(ctrl)
	schemaProvider := mocks.NewMockSchemaProvider(ctrl)
	apiExtensions := mocks.NewMockAPIExtensions(ctrl)

	cluster := cluster.New(
		apiExtensions,
		schemaProvider,
		cluster.WithDatabase(mockDB),
		cluster.WithFileSystem(mockFileSystem),
	)
	if cluster == nil {
		t.Errorf("expected cluster not be nil")
	}
}

// Open

func TestOpen(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mocks.NewMockDriver(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockStore := mocks.NewMockServerStore(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		deps.databaseIO.EXPECT().Create(mockStore).Return(mockDriver, nil),
		deps.nameProvider.EXPECT().DriverName().Return("driver"),
		deps.databaseIO.EXPECT().Drivers().Return([]string{}),
		deps.databaseIO.EXPECT().Register("driver", mockDriver),
		deps.databaseIO.EXPECT().Open("driver", "db.bin?_foreign_keys=1").Return(mockDB, nil),
	)

	err := deps.cluster.Open(mockStore)
	if err != nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := mockDB, deps.cluster.DB(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestOpenWithEmptyName(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mocks.NewMockDriver(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockStore := mocks.NewMockServerStore(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		deps.databaseIO.EXPECT().Create(mockStore).Return(mockDriver, nil),
		deps.nameProvider.EXPECT().DriverName().Return("driver"),
		deps.databaseIO.EXPECT().Drivers().Return([]string{}),
		deps.databaseIO.EXPECT().Register("driver", mockDriver),
		deps.databaseIO.EXPECT().Open("driver", "db.bin?_foreign_keys=1").Return(mockDB, nil),
	)

	err := deps.cluster.Open(mockStore)
	if err != nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := mockDB, deps.cluster.DB(); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestOpenWithErrorFromOpening(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := mocks.NewMockServerStore(ctrl)
	mockDriver := mocks.NewMockDriver(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		deps.databaseIO.EXPECT().Create(mockStore).Return(mockDriver, nil),
		deps.nameProvider.EXPECT().DriverName().Return("driver"),
		deps.databaseIO.EXPECT().Drivers().Return([]string{}),
		deps.databaseIO.EXPECT().Register("driver", mockDriver),
		deps.databaseIO.EXPECT().Open("driver", "db.bin?_foreign_keys=1").Return(nil, errors.New("bad")),
	)

	err := deps.cluster.Open(mockStore)
	if err == nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

// EnsureSchema

func TestEnsureSchema(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := mocks.NewMockServerStore(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockSchema := mocks.NewMockSchema(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		exportClusterOpen(ctrl, mockStore, mockDB, deps.databaseIO, deps.nameProvider),
		deps.apiExtensions.EXPECT().Count().Return(1),
		deps.schemaProvider.EXPECT().Updates().Return([]schema.Update{
			func(tx database.Tx) error { return nil },
		}),
		deps.schemaProvider.EXPECT().Schema().Return(mockSchema),
		mockSchema.EXPECT().File("/path/to/a/dir/patch.global.sql"),
		mockSchema.EXPECT().Check(gomock.Any()),
		mockSchema.EXPECT().Hook(gomock.Any()),
		mockSchema.EXPECT().Ensure(mockDB).Return(0, nil),
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		deps.apiExtensions.EXPECT().Count().Return(1),
		mockTx.EXPECT().Exec(cluster.StmtInitialNode, 1, 1).Return(nil, nil),
		mockTx.EXPECT().Commit().Return(nil),
	)

	err := deps.cluster.Open(mockStore)
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	ok, err := deps.cluster.EnsureSchema("127.0.0.1", "/path/to/a/dir")
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	if expected, actual := true, ok; expected != actual {
		t.Errorf("expected: %t, actual: %t", expected, actual)
	}
}

func TestEnsureSchemaWithRetrySemanticsAroundEnsure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := mocks.NewMockServerStore(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockSchema := mocks.NewMockSchema(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		exportClusterOpen(ctrl, mockStore, mockDB, deps.databaseIO, deps.nameProvider),
		deps.apiExtensions.EXPECT().Count().Return(1),
		deps.schemaProvider.EXPECT().Updates().Return([]schema.Update{
			func(tx database.Tx) error { return nil },
		}),
		deps.schemaProvider.EXPECT().Schema().Return(mockSchema),
		mockSchema.EXPECT().File("/path/to/a/dir/patch.global.sql"),
		mockSchema.EXPECT().Check(gomock.Any()),
		mockSchema.EXPECT().Hook(gomock.Any()),
	)

	mockSchema.EXPECT().Ensure(mockDB).Return(0, errors.New("bad")).Times(11)
	deps.sleeper.EXPECT().Sleep(250 * time.Millisecond).Times(10)

	err := deps.cluster.Open(mockStore)
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	_, err = deps.cluster.EnsureSchema("127.0.0.1", "/path/to/a/dir")
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestEnsureSchemaWithTxBeginFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := mocks.NewMockServerStore(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockSchema := mocks.NewMockSchema(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		exportClusterOpen(ctrl, mockStore, mockDB, deps.databaseIO, deps.nameProvider),
		deps.apiExtensions.EXPECT().Count().Return(1),
		deps.schemaProvider.EXPECT().Updates().Return([]schema.Update{
			func(tx database.Tx) error { return nil },
		}),
		deps.schemaProvider.EXPECT().Schema().Return(mockSchema),
		mockSchema.EXPECT().File("/path/to/a/dir/patch.global.sql"),
		mockSchema.EXPECT().Check(gomock.Any()),
		mockSchema.EXPECT().Hook(gomock.Any()),
		mockSchema.EXPECT().Ensure(mockDB).Return(0, nil),
		mockDB.EXPECT().Begin().Return(mockTx, errors.New("bad")),
	)

	err := deps.cluster.Open(mockStore)
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	_, err = deps.cluster.EnsureSchema("127.0.0.1", "/path/to/a/dir")
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestEnsureSchemaWithTxExecFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := mocks.NewMockServerStore(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockSchema := mocks.NewMockSchema(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		exportClusterOpen(ctrl, mockStore, mockDB, deps.databaseIO, deps.nameProvider),
		deps.apiExtensions.EXPECT().Count().Return(1),
		deps.schemaProvider.EXPECT().Updates().Return([]schema.Update{
			func(tx database.Tx) error { return nil },
		}),
		deps.schemaProvider.EXPECT().Schema().Return(mockSchema),
		mockSchema.EXPECT().File("/path/to/a/dir/patch.global.sql"),
		mockSchema.EXPECT().Check(gomock.Any()),
		mockSchema.EXPECT().Hook(gomock.Any()),
		mockSchema.EXPECT().Ensure(mockDB).Return(0, nil),
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		deps.apiExtensions.EXPECT().Count().Return(1),
		mockTx.EXPECT().Exec(cluster.StmtInitialNode, 1, 1).Return(nil, errors.New("bad")),
		mockTx.EXPECT().Rollback().Return(nil),
	)

	err := deps.cluster.Open(mockStore)
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	_, err = deps.cluster.EnsureSchema("127.0.0.1", "/path/to/a/dir")
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestEnsureSchemaWithTxRollbackFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := mocks.NewMockServerStore(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockSchema := mocks.NewMockSchema(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		exportClusterOpen(ctrl, mockStore, mockDB, deps.databaseIO, deps.nameProvider),
		deps.apiExtensions.EXPECT().Count().Return(1),
		deps.schemaProvider.EXPECT().Updates().Return([]schema.Update{
			func(tx database.Tx) error { return nil },
		}),
		deps.schemaProvider.EXPECT().Schema().Return(mockSchema),
		mockSchema.EXPECT().File("/path/to/a/dir/patch.global.sql"),
		mockSchema.EXPECT().Check(gomock.Any()),
		mockSchema.EXPECT().Hook(gomock.Any()),
		mockSchema.EXPECT().Ensure(mockDB).Return(0, nil),
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		deps.apiExtensions.EXPECT().Count().Return(1),
		mockTx.EXPECT().Exec(cluster.StmtInitialNode, 1, 1).Return(nil, errors.New("bad")),
		mockTx.EXPECT().Rollback().Return(errors.New("fail")),
	)

	err := deps.cluster.Open(mockStore)
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	_, err = deps.cluster.EnsureSchema("127.0.0.1", "/path/to/a/dir")
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestEnsureSchemaWithCommitFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := mocks.NewMockServerStore(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockSchema := mocks.NewMockSchema(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		exportClusterOpen(ctrl, mockStore, mockDB, deps.databaseIO, deps.nameProvider),
		deps.apiExtensions.EXPECT().Count().Return(1),
		deps.schemaProvider.EXPECT().Updates().Return([]schema.Update{
			func(tx database.Tx) error { return nil },
		}),
		deps.schemaProvider.EXPECT().Schema().Return(mockSchema),
		mockSchema.EXPECT().File("/path/to/a/dir/patch.global.sql"),
		mockSchema.EXPECT().Check(gomock.Any()),
		mockSchema.EXPECT().Hook(gomock.Any()),
		mockSchema.EXPECT().Ensure(mockDB).Return(0, nil),
		mockDB.EXPECT().Begin().Return(mockTx, nil),
		deps.apiExtensions.EXPECT().Count().Return(1),
		mockTx.EXPECT().Exec(cluster.StmtInitialNode, 1, 1).Return(nil, nil),
		mockTx.EXPECT().Commit().Return(errors.New("bad")),
	)

	err := deps.cluster.Open(mockStore)
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
	_, err = deps.cluster.EnsureSchema("127.0.0.1", "/path/to/a/dir")
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

// hook

func TestHook(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		deps.fileSystem.EXPECT().CopyDir("/path/to/a/dir/global", "/path/to/a/dir/global.bak").Return(nil),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
}

func TestHookWithNoUpdates(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
}

func TestHookWithStmtSchemaTableExistsQueryFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, errors.New("bad")),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestHookWithStmtSchemaTableExistsAndNoRows(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "schema table query returned no rows", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestHookWithStmtSchemaTableExistsAndScanFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestHookWithStmtCountAndFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, errors.New("bad")),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestHookWithStmtCountAndNoRows(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "no rows returned", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestHookWithStmtCountAndScanFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "failed to scan count column", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestHookWithStmtCountAndRowCheckingFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "more than one row returned", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestHookWithStmtCountAndErrReturnsAnError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestHookWithBackupFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		deps.fileSystem.EXPECT().CopyDir("/path/to/a/dir/global", "/path/to/a/dir/global.bak").Return(errors.New("bad")),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestHookWithMoreClusterNodes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	deps := createClusterDeps(t, ctrl)
	gomock.InOrder(
		mockTx.EXPECT().Query(schema.StmtSchemaTableExists).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(2)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Hook(ctx, deps.fileSystem, "/path/to/a/dir", 1, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "found more than one unclustered nodes", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

// check

func TestCheck(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(cluster.StmtUpdateNodeVersion, 1, 10, "0.0.0.0").Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil),
		mockTx.EXPECT().Query(cluster.StmtSelectNodesVersions).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1), ScanMatcher(10)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
}

func TestCheckWithCurrentEqualsZero(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)

	err := cluster.Check(ctx, "10.0.0.1", 0, 1, 10, mockTx)
	if err != nil {
		t.Errorf("expected err to not be nil: got %v", err)
	}
}

func TestCheckWithUnclusterNodesCountGreaterThanOne(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(2)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "found more than one unclustered nodes", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestCheckWithUnclusterNodesCountScanFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(2)).Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "failed to scan count column", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestCheckWithUnclusterNodesCountErrFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(2)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestCheckWithUpdateNodeVersionExecFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(cluster.StmtUpdateNodeVersion, 1, 10, "0.0.0.0").Return(nil, errors.New("bad")),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestCheckWithUpdateNodeVersionRowsAffectedFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(cluster.StmtUpdateNodeVersion, 1, 10, "0.0.0.0").Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(0), errors.New("bad")),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestCheckWithUpdateNodeVersionQueryFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(cluster.StmtUpdateNodeVersion, 1, 10, "0.0.0.0").Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil),
		mockTx.EXPECT().Query(cluster.StmtSelectNodesVersions).Return(mockRows, errors.New("bad")),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestCheckWithUpdateNodeVersionScanFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(cluster.StmtUpdateNodeVersion, 1, 10, "0.0.0.0").Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil),
		mockTx.EXPECT().Query(cluster.StmtSelectNodesVersions).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1), ScanMatcher(10)).Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestCheckWithUpdateNodeVersionErrFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(cluster.StmtUpdateNodeVersion, 1, 10, "0.0.0.0").Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil),
		mockTx.EXPECT().Query(cluster.StmtSelectNodesVersions).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1), ScanMatcher(10)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(errors.New("bad")),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "bad", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestCheckWithUpdateNodeVersionAffectedRowsGreaterThanOne(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(cluster.StmtUpdateNodeVersion, 1, 10, "0.0.0.0").Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(2), nil),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "updated 2 rows instead of 1", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestCheckWithFallenBehindNode(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(cluster.StmtUpdateNodeVersion, 1, 10, "0.0.0.0").Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil),
		mockTx.EXPECT().Query(cluster.StmtSelectNodesVersions).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(10), ScanMatcher(10)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "this node's version is behind, please upgrade", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestCheckWithInconsistentVersions(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(cluster.StmtUpdateNodeVersion, 1, 10, "0.0.0.0").Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil),
		mockTx.EXPECT().Query(cluster.StmtSelectNodesVersions).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(12), ScanMatcher(0)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 1, 10, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "nodes have inconsistent versions", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}

func TestCheckWithFallenComrades(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := &cluster.Context{}
	mockTx := mocks.NewMockTx(ctrl)
	mockRows := mocks.NewMockRows(ctrl)
	mockResult := mocks.NewMockResult(ctrl)

	gomock.InOrder(
		mockTx.EXPECT().Query(query.StmtCount("nodes", "address='0.0.0.0'")).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
		mockTx.EXPECT().Exec(cluster.StmtUpdateNodeVersion, 2, 11, "0.0.0.0").Return(mockResult, nil),
		mockResult.EXPECT().RowsAffected().Return(int64(1), nil),
		mockTx.EXPECT().Query(cluster.StmtSelectNodesVersions).Return(mockRows, nil),
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(ScanMatcher(1), ScanMatcher(10)).Return(nil),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Err().Return(nil),
		mockRows.EXPECT().Close().Return(nil),
	)

	err := cluster.Check(ctx, "10.0.0.1", 1, 2, 11, mockTx)
	if err == nil {
		t.Errorf("expected err to be nil: got %v", err)
	}
	if expected, actual := "schema check gracefully aborted", errors.Cause(err).Error(); expected != actual {
		t.Errorf("expected: %s, actual: %s", expected, actual)
	}
}
