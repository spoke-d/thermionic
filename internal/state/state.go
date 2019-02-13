package state

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

// Node mediates access to the data stored in the node-local SQLite database.
type Node interface {
	database.DBAccessor
	db.NodeOpener
	db.NodeTransactioner

	// Dir returns the directory of the underlying database file.
	Dir() string

	// Close the database facade.
	Close() error
}

// Cluster mediates access to data stored in the cluster dqlite database.
type Cluster interface {
	database.DBAccessor
	db.ClusterOpener
	db.ClusterExclusiveLocker
	db.ClusterTransactioner

	// NodeID sets the the node NodeID associated with this cluster instance. It's used for
	// backward-compatibility of all db-related APIs that were written before
	// clustering and don't accept a node NodeID, so in those cases we automatically
	// use this value as implicit node NodeID.
	NodeID(int64)

	// SchemaVersion returns the underlying schema version for the cluster
	SchemaVersion() int

	// Close the database facade.
	Close() error
}

// State is a gateway to the two main stateful components, the database
// and the operating system. It's typically used by model entities such as
// containers, volumes, etc. in order to perform changes.
type State struct {
	node    Node
	cluster Cluster
	os      OS
}

// NewState returns a new State object with the given database and operating
// system components.
func NewState(options ...Option) *State {
	opts := newOptions()
	for _, option := range options {
		option(opts)
	}

	return &State{
		node:    opts.node,
		cluster: opts.cluster,
		os:      opts.os,
	}
}

// Node returns the underlying Node
func (s *State) Node() Node {
	return s.node
}

// Cluster returns the underlying Cluster
func (s *State) Cluster() Cluster {
	return s.cluster
}

// OS returns the underlying OS values
func (s *State) OS() OS {
	return s.os
}

// UnsafeSetCluster modifies the underlying state to set the cluster upon the
// state
func UnsafeSetCluster(state *State, cluster Cluster) {
	state.cluster = cluster
}
