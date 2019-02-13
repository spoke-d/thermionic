package services

import (
	"os/user"

	"github.com/spoke-d/thermionic/internal/db"
	"github.com/spoke-d/thermionic/internal/db/database"
)

// OS is a high-level facade for accessing all operating-system
// level functionality that therm uses.
type OS interface {

	// LocalDatabasePath returns the path of the local database file.
	LocalDatabasePath() string

	// GlobalDatabaseDir returns the path of the global database directory.
	GlobalDatabaseDir() string

	// GlobalDatabasePath returns the path of the global database SQLite file
	// managed by dqlite.
	GlobalDatabasePath() string

	// VarDir represents the Data directory (e.g. /var/lib/therm/).
	VarDir() string

	// Hostname returns the host name reported by the kernel.
	Hostname() (string, error)

	// HostNames will generate a list of names for which the certificate will be
	// valid.
	// This will include the hostname and ip address
	HostNames() ([]string, error)

	// User returns the current user.
	User() (*user.User, error)
}

// State is a gateway to the two main stateful components, the database
// and the operating system. It's typically used by model entities in order to
// perform changes.
type State interface {
	// Node returns the underlying Node
	Node() Node

	// Cluster returns the underlying Cluster
	Cluster() Cluster

	// OS returns the underlying OS values
	OS() OS
}

// Node mediates access to the data stored in the node-local SQLite database.
type Node interface {
	db.NodeTransactioner
}

// Cluster mediates access to data stored in the cluster dqlite database.
type Cluster interface {
	db.ClusterOpener
	db.ClusterTransactioner
	db.ClusterExclusiveLocker

	// NodeID sets the the node NodeID associated with this cluster instance.
	// It's used for backward-compatibility of all db-related APIs that were
	// written before clustering and don't accept a node NodeID, so in those
	// cases we automatically use this value as implicit node NodeID.
	NodeID(int64)

	// DB returns the underlying db
	DB() database.DB

	// SchemaVersion returns the underlying schema version for the cluster
	SchemaVersion() int

	// Close the database facade.
	Close() error
}
