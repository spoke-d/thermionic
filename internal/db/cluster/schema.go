package cluster

const freshSchema = `
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

INSERT INTO schema (version, updated_at) VALUES (2, strftime("%s"));
`
