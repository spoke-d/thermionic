package db_test

import (
	"testing"
	"time"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/mocks"
)

func TestClusterOpen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ch := make(chan time.Time, 1)
	input := []db.NodeInfo{
		{
			ID:            0,
			Name:          "node1",
			Address:       "0.0.0.0",
			Description:   "node 1",
			Schema:        1,
			APIExtensions: 1,
			Heartbeat:     time.Now(),
		},
	}

	mockQueryCluster := mocks.NewMockQueryCluster(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockSleeper := mocks.NewMockSleeper(ctrl)
	mockStore := mocks.NewMockServerStore(ctrl)

	gomock.InOrder(
		mockQueryCluster.EXPECT().Open(gomock.Any()).Return(nil),
		mockClock.EXPECT().After(gomock.Any()).Return(ch),
		mockQueryCluster.EXPECT().DB().Return(mockDB),
		mockDB.EXPECT().Ping().Return(nil),
		mockQueryCluster.EXPECT().EnsureSchema("0.0.0.0", "/path/to/a/dir").Return(true, nil),
		mockQueryCluster.EXPECT().DB().Return(mockDB),
		mockTransaction.EXPECT().Transaction(mockDB, TransactionMatcher(mockTx)).Return(nil),
	)

	// clusterTx controller because of the mutex with in the original controller
	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockQuery := mocks.NewMockQuery(ctrl2)
	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		`SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? ORDER BY id`,
		0,
	).Return(nil)

	mockClusterTxProvider := mocks.NewMockClusterTxProvider(ctrl2)
	mockClusterTxProvider.EXPECT().New(gomock.Any(), gomock.Any()).DoAndReturn(func(tx database.Tx, nodeID int64) *db.ClusterTx {
		return db.NewClusterTxWithQuery(tx, nodeID, mockQuery)
	})

	cluster := db.NewCluster(mockQueryCluster,
		db.WithTransactionForCluster(mockTransaction),
		db.WithClockForCluster(mockClock),
		db.WithSleeperForCluster(mockSleeper),
		db.WithClusterTxProviderForCluster(mockClusterTxProvider),
	)
	err := cluster.Open(mockStore, "0.0.0.0", "/path/to/a/dir", time.Minute*1)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterOpenWithMultipleNodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ch := make(chan time.Time, 1)
	input := []db.NodeInfo{
		{
			ID:            0,
			Name:          "node1",
			Address:       "0.0.0.0",
			Description:   "node 1",
			Schema:        1,
			APIExtensions: 1,
			Heartbeat:     time.Now(),
		},
		{
			ID:            1,
			Name:          "node2",
			Address:       "0.0.0.1",
			Description:   "node 2",
			Schema:        1,
			APIExtensions: 1,
			Heartbeat:     time.Now(),
		},
	}

	mockQueryCluster := mocks.NewMockQueryCluster(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockSleeper := mocks.NewMockSleeper(ctrl)
	mockStore := mocks.NewMockServerStore(ctrl)

	gomock.InOrder(
		mockQueryCluster.EXPECT().Open(gomock.Any()).Return(nil),
		mockClock.EXPECT().After(gomock.Any()).Return(ch),
		mockQueryCluster.EXPECT().DB().Return(mockDB),
		mockDB.EXPECT().Ping().Return(nil),
		mockQueryCluster.EXPECT().EnsureSchema("0.0.0.0", "/path/to/a/dir").Return(true, nil),
		mockQueryCluster.EXPECT().DB().Return(mockDB),
		mockTransaction.EXPECT().Transaction(mockDB, TransactionMatcher(mockTx)).Return(nil),
	)

	// clusterTx controller because of the mutex with in the original controller
	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockQuery := mocks.NewMockQuery(ctrl2)
	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		`SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? ORDER BY id`,
		0,
	).Return(nil)

	mockClusterTxProvider := mocks.NewMockClusterTxProvider(ctrl2)
	mockClusterTxProvider.EXPECT().New(gomock.Any(), gomock.Any()).DoAndReturn(func(tx database.Tx, nodeID int64) *db.ClusterTx {
		return db.NewClusterTxWithQuery(tx, nodeID, mockQuery)
	})

	cluster := db.NewCluster(mockQueryCluster,
		db.WithTransactionForCluster(mockTransaction),
		db.WithClockForCluster(mockClock),
		db.WithSleeperForCluster(mockSleeper),
		db.WithClusterTxProviderForCluster(mockClusterTxProvider),
	)
	err := cluster.Open(mockStore, "0.0.0.0", "/path/to/a/dir", time.Minute*1)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterOpenWithMultipleNodesWithNoMatchingAddress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ch := make(chan time.Time, 1)
	input := []db.NodeInfo{
		{
			ID:            0,
			Name:          "node1",
			Address:       "0.0.0.12",
			Description:   "node 1",
			Schema:        1,
			APIExtensions: 1,
			Heartbeat:     time.Now(),
		},
		{
			ID:            1,
			Name:          "node2",
			Address:       "0.0.0.1",
			Description:   "node 2",
			Schema:        1,
			APIExtensions: 1,
			Heartbeat:     time.Now(),
		},
	}

	mockQueryCluster := mocks.NewMockQueryCluster(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockSleeper := mocks.NewMockSleeper(ctrl)
	mockStore := mocks.NewMockServerStore(ctrl)

	matcher := TransactionMatcher(mockTx)

	gomock.InOrder(
		mockQueryCluster.EXPECT().Open(gomock.Any()).Return(nil),
		mockClock.EXPECT().After(gomock.Any()).Return(ch),
		mockQueryCluster.EXPECT().DB().Return(mockDB),
		mockDB.EXPECT().Ping().Return(nil),
		mockQueryCluster.EXPECT().EnsureSchema("0.0.0.0", "/path/to/a/dir").Return(true, nil),
		mockQueryCluster.EXPECT().DB().Return(mockDB),
		mockTransaction.EXPECT().Transaction(mockDB, matcher).Return(nil),
	)

	// clusterTx controller because of the mutex with in the original controller
	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockQuery := mocks.NewMockQuery(ctrl2)
	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		`SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? ORDER BY id`,
		0,
	).Return(nil)

	mockClusterTxProvider := mocks.NewMockClusterTxProvider(ctrl2)
	mockClusterTxProvider.EXPECT().New(gomock.Any(), gomock.Any()).DoAndReturn(func(tx database.Tx, nodeID int64) *db.ClusterTx {
		return db.NewClusterTxWithQuery(tx, nodeID, mockQuery)
	})

	cluster := db.NewCluster(mockQueryCluster,
		db.WithTransactionForCluster(mockTransaction),
		db.WithClockForCluster(mockClock),
		db.WithSleeperForCluster(mockSleeper),
		db.WithClusterTxProviderForCluster(mockClusterTxProvider),
	)
	_ = cluster.Open(mockStore, "0.0.0.0", "/path/to/a/dir", time.Minute*1)
	if matcher.Err() == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterOpenWithVersionMissmatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ch := make(chan time.Time, 1)
	input := []db.NodeInfo{
		{
			ID:            0,
			Name:          "node1",
			Address:       "0.0.0.0",
			Description:   "node 1",
			Schema:        1,
			APIExtensions: 1,
			Heartbeat:     time.Now(),
		},
	}

	mockQueryCluster := mocks.NewMockQueryCluster(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockSleeper := mocks.NewMockSleeper(ctrl)
	mockStore := mocks.NewMockServerStore(ctrl)

	gomock.InOrder(
		mockQueryCluster.EXPECT().Open(gomock.Any()).Return(nil),
		mockClock.EXPECT().After(gomock.Any()).Return(ch),
		mockQueryCluster.EXPECT().DB().Return(mockDB),
		mockDB.EXPECT().Ping().Return(nil),
		mockQueryCluster.EXPECT().EnsureSchema("0.0.0.0", "/path/to/a/dir").Return(false, nil),
		mockQueryCluster.EXPECT().DB().Return(mockDB),
		mockTransaction.EXPECT().Transaction(mockDB, TransactionMatcher(mockTx)).Return(nil),
	)

	// clusterTx controller because of the mutex with in the original controller
	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockQuery := mocks.NewMockQuery(ctrl2)
	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		`SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? ORDER BY id`,
		0,
	).Return(nil)

	mockClusterTxProvider := mocks.NewMockClusterTxProvider(ctrl2)
	mockClusterTxProvider.EXPECT().New(gomock.Any(), gomock.Any()).DoAndReturn(func(tx database.Tx, nodeID int64) *db.ClusterTx {
		return db.NewClusterTxWithQuery(tx, nodeID, mockQuery)
	})

	cluster := db.NewCluster(mockQueryCluster,
		db.WithTransactionForCluster(mockTransaction),
		db.WithClockForCluster(mockClock),
		db.WithSleeperForCluster(mockSleeper),
		db.WithClusterTxProviderForCluster(mockClusterTxProvider),
	)
	err := cluster.Open(mockStore, "0.0.0.0", "/path/to/a/dir", time.Minute*1)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
	if expected, actual := db.ErrSomeNodesAreBehind, errors.Cause(err); expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

func TestClusterOpenWithOpenFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockQueryCluster := mocks.NewMockQueryCluster(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockSleeper := mocks.NewMockSleeper(ctrl)
	mockStore := mocks.NewMockServerStore(ctrl)

	mockQueryCluster.EXPECT().Open(gomock.Any()).Return(errors.New("bad"))

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockClusterTxProvider := mocks.NewMockClusterTxProvider(ctrl2)

	cluster := db.NewCluster(mockQueryCluster,
		db.WithTransactionForCluster(mockTransaction),
		db.WithClockForCluster(mockClock),
		db.WithSleeperForCluster(mockSleeper),
		db.WithClusterTxProviderForCluster(mockClusterTxProvider),
	)
	err := cluster.Open(mockStore, "0.0.0.0", "/path/to/a/dir", time.Minute*1)
	if err == nil {
		t.Errorf("expected err not to be nil")
	}
}

func TestClusterOpenWithPingFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ch := make(chan time.Time, 1)
	input := []db.NodeInfo{
		{
			ID:            0,
			Name:          "node1",
			Address:       "0.0.0.0",
			Description:   "node 1",
			Schema:        1,
			APIExtensions: 1,
			Heartbeat:     time.Now(),
		},
	}

	mockQueryCluster := mocks.NewMockQueryCluster(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockSleeper := mocks.NewMockSleeper(ctrl)
	mockStore := mocks.NewMockServerStore(ctrl)

	gomock.InOrder(
		mockQueryCluster.EXPECT().Open(gomock.Any()).Return(nil),
		mockClock.EXPECT().After(gomock.Any()).Return(ch),
		InOrder(
			mockQueryCluster.EXPECT().DB().Return(mockDB),
			mockDB.EXPECT().Ping().Return(dqlite.ErrNoAvailableLeader),
			mockSleeper.EXPECT().Sleep(2*time.Second),
		),
		InOrder(
			mockQueryCluster.EXPECT().DB().Return(mockDB),
			mockDB.EXPECT().Ping().Return(dqlite.ErrNoAvailableLeader),
			mockSleeper.EXPECT().Sleep(2*time.Second),
		),
		InOrder(
			mockQueryCluster.EXPECT().DB().Return(mockDB),
			mockDB.EXPECT().Ping().Return(dqlite.ErrNoAvailableLeader),
			mockSleeper.EXPECT().Sleep(2*time.Second),
		),
		InOrder(
			mockQueryCluster.EXPECT().DB().Return(mockDB),
			mockDB.EXPECT().Ping().Return(dqlite.ErrNoAvailableLeader),
			mockSleeper.EXPECT().Sleep(2*time.Second),
		),
		InOrder(
			mockQueryCluster.EXPECT().DB().Return(mockDB),
			mockDB.EXPECT().Ping().Return(dqlite.ErrNoAvailableLeader),
			mockSleeper.EXPECT().Sleep(2*time.Second),
		),
		InOrder(
			mockQueryCluster.EXPECT().DB().Return(mockDB),
			mockDB.EXPECT().Ping().Return(dqlite.ErrNoAvailableLeader),
			mockSleeper.EXPECT().Sleep(2*time.Second),
		),
		InOrder(
			mockQueryCluster.EXPECT().DB().Return(mockDB),
			mockDB.EXPECT().Ping().Return(dqlite.ErrNoAvailableLeader),
			mockSleeper.EXPECT().Sleep(2*time.Second),
		),
		mockQueryCluster.EXPECT().DB().Return(mockDB),
		mockDB.EXPECT().Ping().Return(nil),
		mockQueryCluster.EXPECT().EnsureSchema("0.0.0.0", "/path/to/a/dir").Return(true, nil),
		mockQueryCluster.EXPECT().DB().Return(mockDB),
		mockTransaction.EXPECT().Transaction(mockDB, TransactionMatcher(mockTx)).Return(nil),
	)

	// clusterTx controller because of the mutex with in the original controller
	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockQuery := mocks.NewMockQuery(ctrl2)
	mockQuery.EXPECT().SelectObjects(
		mockTx,
		NodeInfoDestSelectObjectsMatcher(input),
		`SELECT id, name, address, description, schema, api_extensions, heartbeat FROM nodes WHERE pending=? ORDER BY id`,
		0,
	).Return(nil)
	mockClusterTxProvider := mocks.NewMockClusterTxProvider(ctrl2)
	mockClusterTxProvider.EXPECT().New(gomock.Any(), gomock.Any()).DoAndReturn(func(tx database.Tx, nodeID int64) *db.ClusterTx {
		return db.NewClusterTxWithQuery(tx, nodeID, mockQuery)
	})

	cluster := db.NewCluster(mockQueryCluster,
		db.WithTransactionForCluster(mockTransaction),
		db.WithClockForCluster(mockClock),
		db.WithSleeperForCluster(mockSleeper),
		db.WithClusterTxProviderForCluster(mockClusterTxProvider),
	)
	err := cluster.Open(mockStore, "0.0.0.0", "/path/to/a/dir", time.Minute*1)
	if err != nil {
		t.Errorf("expected err to be nil")
	}
}

func TestClusterOpenWithEnsureSchemaFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ch := make(chan time.Time, 1)

	mockQueryCluster := mocks.NewMockQueryCluster(ctrl)
	mockTransaction := mocks.NewMockTransaction(ctrl)
	mockDB := mocks.NewMockDB(ctrl)
	mockClock := mocks.NewMockClock(ctrl)
	mockSleeper := mocks.NewMockSleeper(ctrl)
	mockStore := mocks.NewMockServerStore(ctrl)

	gomock.InOrder(
		mockQueryCluster.EXPECT().Open(gomock.Any()).Return(nil),
		mockClock.EXPECT().After(gomock.Any()).Return(ch),
		mockQueryCluster.EXPECT().DB().Return(mockDB),
		mockDB.EXPECT().Ping().Return(nil),
		mockQueryCluster.EXPECT().EnsureSchema("0.0.0.0", "/path/to/a/dir").Return(true, errors.New("bad")),
	)

	ctrl2 := gomock.NewController(t)
	defer ctrl2.Finish()

	mockClusterTxProvider := mocks.NewMockClusterTxProvider(ctrl2)
	cluster := db.NewCluster(mockQueryCluster,
		db.WithTransactionForCluster(mockTransaction),
		db.WithClockForCluster(mockClock),
		db.WithSleeperForCluster(mockSleeper),
		db.WithClusterTxProviderForCluster(mockClusterTxProvider),
	)
	err := cluster.Open(mockStore, "0.0.0.0", "/path/to/a/dir", time.Minute*1)
	if err == nil {
		t.Errorf("expected err to be nil")
	}
}
