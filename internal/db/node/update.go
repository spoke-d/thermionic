package node

import (
	"github.com/spoke-d/thermionic/internal/db/database"
	"github.com/spoke-d/thermionic/internal/db/schema"
	"github.com/spoke-d/thermionic/internal/fsys"
)

type schemaProvider struct {
	fileSystem fsys.FileSystem
}

func (s schemaProvider) Schema() Schema {
	schema := schema.New(s.fileSystem, s.Updates())
	schema.Fresh(freshSchema)
	return schema
}

func (s schemaProvider) Updates() []schema.Update {
	return []schema.Update{
		updateFromV0,
	}
}

func updateFromV0(tx database.Tx) error {
	// v0..v1 the dawn of clustering
	stmt := `
CREATE TABLE config (
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	key VARCHAR(255) NOT NULL,
	value TEXT,
	UNIQUE (key)
);
CREATE TABLE patches (
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	name VARCHAR(255) NOT NULL,
	applied_at DATETIME NOT NULL,
	UNIQUE (name)
);
CREATE TABLE raft_nodes (
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	address TEXT NOT NULL,
	UNIQUE (address)
);
`
	_, err := tx.Exec(stmt)
	return err
}
