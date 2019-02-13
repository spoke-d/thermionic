package node

import (
	"database/sql"
	"database/sql/driver"

	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/pkg/errors"
)

type databaseIO struct {
	// TODO (Simon): Add metrics here
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
