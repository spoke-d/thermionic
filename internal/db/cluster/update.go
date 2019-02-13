package cluster

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
		updateFromV1,
	}
}

// NewSchema creates a schema for updating a cluster node
func NewSchema(fileSysyem fsys.FileSystem) SchemaProvider {
	return &schemaProvider{
		fileSystem: fileSysyem,
	}
}

// FreshSchema returns the fresh schema definition of the global database.
func FreshSchema() string {
	return freshSchema
}

func updateFromV1(tx database.Tx) error {
	// v1..v2 the dawn of discovery
	stmt := `
CREATE TABLE services (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	address TEXT NOT NULL,
	daemon_address TEXT NOT NULL,
	daemon_nonce TEXT NOT NULL,
	heartbeat DATETIME DEFAULT CURRENT_TIMESTAMP,
	UNIQUE (name),
	UNIQUE (address),
	UNIQUE (daemon_address)
);
`
	_, err := tx.Exec(stmt)
	return err
}

func updateFromV0(tx database.Tx) error {
	// v0..v1 the dawn of clustering
	stmt := `
CREATE TABLE config (
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	key TEXT NOT NULL,
	value TEXT,
	UNIQUE (key)
);

CREATE TABLE nodes (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    address TEXT NOT NULL,
    schema INTEGER NOT NULL,
    api_extensions INTEGER NOT NULL,
    heartbeat DATETIME DEFAULT CURRENT_TIMESTAMP,
    pending INTEGER NOT NULL DEFAULT 0,
    UNIQUE (name),
    UNIQUE (address)
);

CREATE TABLE operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    uuid TEXT NOT NULL,
	node_id TEXT NOT NULL,
	type TEXT NOT NULL,
    UNIQUE (uuid),
    FOREIGN KEY (node_id) REFERENCES nodes (id) ON DELETE CASCADE
);

CREATE TABLE tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    uuid TEXT NOT NULL,
	node_id TEXT NOT NULL,
	query TEXT NOT NULL,
	schedule INTEGER NOT NULL,
    result TEXT NOT NULL,
    status INTEGER NOT NULL,
    UNIQUE (uuid),
    FOREIGN KEY (node_id) REFERENCES nodes (id) ON DELETE CASCADE
);
`
	_, err := tx.Exec(stmt)
	return err
}
