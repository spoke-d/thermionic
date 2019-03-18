package cluster

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/CanonicalLtd/go-dqlite"
	"github.com/pkg/errors"
	"github.com/spoke-d/thermionic/internal/db/database"
)

// The following will shim the database to enable better logging and metrics
// at the query sites.

type databaseIO struct {
	// TODO (Simon): Add metrics here
}

func (databaseIO) Create(store ServerStore, options ...dqlite.DriverOption) (driver.Driver, error) {
	return dqlite.NewDriver(makeServerStore(store), options...)
}

func (databaseIO) Register(driverName string, driver driver.Driver) {
	sql.Register(driverName, driver)
}

func (databaseIO) Drivers() []string {
	return sql.Drivers()
}

func (databaseIO) Open(driverName, dataSourceName string) (database.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return database.ShimDB(db, err)
}

type serverStoreShim struct {
	store ServerStore
}

func makeServerStore(store ServerStore) dqlite.ServerStore {
	return serverStoreShim{
		store: store,
	}
}

// Get return the list of known servers.
func (s serverStoreShim) Get(ctx context.Context) ([]dqlite.ServerInfo, error) {
	info, err := s.store.Get(ctx)
	if err != nil {
		return nil, err
	}
	res := make([]dqlite.ServerInfo, len(info))
	for k, v := range info {
		res[k] = dqlite.ServerInfo{
			ID:      v.ID,
			Address: v.Address,
		}
	}
	return res, nil
}

// Set updates the list of known cluster servers.
func (s serverStoreShim) Set(ctx context.Context, info []dqlite.ServerInfo) error {
	res := make([]ServerInfo, len(info))
	for k, v := range info {
		res[k] = ServerInfo{
			ID:      v.ID,
			Address: v.Address,
		}
	}
	return s.store.Set(ctx, res)
}
